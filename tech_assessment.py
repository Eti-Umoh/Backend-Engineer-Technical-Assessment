# SmallWorld
# Backend Engineer — Technical Assessment


# Time allowed: 90 minutes
# Total points: 100
# Format: Written answers + one short coding task. Submit as a single document or repo.

# These questions are designed to test real production experience. Write from what you know — short, direct answers score higher than long ones that avoid the question. There are no trick questions, but there are wrong answers.


# Section 1 — Debug This  [30 pts]
# Read each snippet carefully. Identify the bug, explain why it is a bug in production, and write the fix.

# Q1  Celery task silently fails on retry   [10 pts]
# This Celery task processes a video upload. On production, it sometimes fails and retries — but after 3 retries the reward is never updated and no error appears in Sentry. Why? Fix it.

@shared_task(bind=True, max_retries=3)
def process_video(self, video_id):
    try:
        video = PostVideo.objects.get(id=video_id)
        result = run_ffmpeg(video.file_path)
        video.status = 'done'
        video.save()
    except PostVideo.DoesNotExist:
        return
    except Exception as e:
        self.retry(exc=e, countdown=30)

Q1_ANSWER = """
Q1 — Why the task fails silently, and the fix
The task never reports an error because all three of its exit paths are silent.
1. except PostVideo.DoesNotExist: return swallows the most likely failure.
    In production this branch usually fires because the task was dispatched from inside an open transaction (or a view under ATOMIC_REQUESTS),
    so the worker picks up the message before the row is committed. Replica lag causes the same thing.
    The task returns cleanly, Celery marks it SUCCESS, and the record keeps its old status. Nothing is logged, so nothing reaches Sentry.
2. Retry exhaustion leaves no terminal state.
    self.retry(exc=e) re-raises the original exception once retries run out, but the code never sets a failed status.
    A permanently failed video is indistinguishable from one still processing.
3. Celery errors may not be wired to Sentry at all.
    If only DjangoIntegration is installed, task failures aren't captured. And with no time limits on an ffmpeg call,
    a hung encode or an OOM kill takes the worker process down with SIGKILL — the except block never runs, so there is no retry and no report.
    """

#FIX:
import logging
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded

logger = logging.getLogger(__name__)

@shared_task(
    bind=True, max_retries=3,
    acks_late=True, reject_on_worker_lost=True,
    soft_time_limit=600, time_limit=660,
)
def process_video(self, video_id):
    try:
        video = PostVideo.objects.get(id=video_id)
    except PostVideo.DoesNotExist as exc:
        logger.warning(
            "PostVideo %s not found (attempt %s)", video_id, self.request.retries
        )
        raise self.retry(exc=exc, countdown=10)

    try:
        run_ffmpeg(video.file_path)
    except SoftTimeLimitExceeded:
        PostVideo.objects.filter(pk=video_id).update(status="failed")
        logger.exception("ffmpeg exceeded soft time limit for video %s", video_id)
        raise
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            PostVideo.objects.filter(pk=video_id).update(status="failed")
            logger.exception("process_video giving up on video %s", video_id)
            raise
        raise self.retry(exc=exc, countdown=30)

    PostVideo.objects.filter(pk=video_id).update(status="done")



# Q2  Race condition in reward approval   [10 pts]
# This is the admin endpoint for approving a cash reward. A supervisor reported that occasionally two transfers are made for the same reward. What is the exact cause and how do you fix it with minimal code change?

def approve_reward(request, reward_id):
    reward = Reward.objects.get(pk=reward_id)
    if reward.status != 'claimed':
        return Response({'error': 'Not claimable'}, status=400)
    
    result = PaystackService.initiate_transfer(
        amount=reward.amount,
        recipient=reward.paystack_recipient_code,
    )
    reward.status = 'approved'
    reward.transfer_code = result['transfer_code']
    reward.save()
    return Response({'detail': 'Approved'})

Q2_ANSWER= """
Q2 — Exact cause:
Check-then-act (TOCTOU). The status read and the status write are separate statements
with no row lock and no transaction, and a slow network call sits in the gap:

    T1  SELECT -> 'claimed', guard passes
    T2  SELECT -> 'claimed', guard passes too (T1 hasn't written yet)
    T1  Paystack transfer #1
    T2  Paystack transfer #2        <- money leaves twice
    T1  UPDATE transfer_code = A
    T2  UPDATE transfer_code = B    <- A is lost

The window is the full duration of the Paystack call, not microseconds. That is why it
happens "occasionally" in production and never in testing.
Two more bugs on the same lines:
- reward.save() writes every column, so T2 clobbers T1's transfer_code. Reconciliation
  against Paystack then cannot even see that transfer #1 happened.
- .get() on a bad id is an uncaught 500, not a 404.

My fix with Minimal code change: transaction + select_for_update(), so the second request blocks until the
first commits and then correctly fails the guard. It must be inside an atomic block,
and it needs Postgres/MySQL — on SQLite it silently does nothing, so test it against
the real engine.
"""

#FIX:
from django.db import transaction
from django.shortcuts import get_object_or_404

@transaction.atomic
def approve_reward(request, reward_id):
    reward = get_object_or_404(Reward.objects.select_for_update(), pk=reward_id)

    if reward.status != 'claimed':
        return Response({'error': 'Not claimable'}, status=400)

    result = PaystackService.initiate_transfer(
        amount=reward.amount,
        recipient=reward.paystack_recipient_code,
        # Deterministic reference: even if the lock is bypassed (replayed request,
        # a retry after a timeout), Paystack itself rejects the duplicate.
        reference=f'reward-{reward.pk}',
    )

    reward.status = 'approved'
    reward.transfer_code = result['transfer_code']
    reward.save(update_fields=['status', 'transfer_code'])
    return Response({'detail': 'Approved'})

# Trade-off I would raise in review:
# This holds the lock and a DB connection for the length of the Paystack call. Fine for
# a low-volume admin action — the lock is per-row, so the only thing it blocks is a
# duplicate approval of the same reward. At higher volume I would claim the row with a
# conditional UPDATE, commit, then transfer outside the transaction:
#
#     claimed = Reward.objects.filter(pk=reward_id, status='claimed').update(
#         status='approving')
#     if not claimed:
#         return Response({'error': 'Not claimable'}, status=400)
#
# UPDATE ... WHERE status='claimed' is atomic in the DB: exactly one caller gets
# rowcount 1. The cost is an 'approving' state needing a sweeper to settle rows whose
# process died mid-transfer, by querying Paystack for our reference. For money flows the
# state machine is what makes the operation safe to retry — not the lock.



# Q3  Migration will fail on a live table   [10 pts]
# A colleague wrote this migration to add a field to a table that already has 500,000 rows in production. What will happen when this runs on prod? How do you fix it without downtime?

class Migration(migrations.Migration):
    dependencies = [('post', '0059_previous')]
    operations = [
        migrations.AddField(
            model_name='post',
            name='content_hash',
            field=models.CharField(max_length=64, unique=True),
        )
    ]

Q3_ANSWER = """
Q3 — When this runs on prod It fails, Every problem here is a function of the 500,000 rows.

1. It fails on the NOT NULL. No default, no null=True, blank=False — Django has no
   value to backfill with, and Postgres raises 'column "content_hash" contains null
   values'. On Postgres DDL is transactional so it rolls back cleanly; on MySQL it is
   not, and a multi-operation migration can leave the schema half applied.

2. Before it even fails it takes an ACCESS EXCLUSIVE lock. ALTER TABLE waits for every
   in-flight query on post_post, and every new query queues behind it. One slow
   analytics SELECT turns a millisecond DDL into a site-wide stall. The outage comes
   from the lock queue, not from the ALTER.

3. If someone "fixes" it with default='', the unique index fails instead — 500k rows
   all holding ''. You cannot escape that with a default: a column default is a
   constant, and uniqueness needs per-row values.

4. Even with valid distinct data, a plain unique constraint builds the index under a
   SHARE lock, blocking every write to post_post for tens of seconds.

Here are four steps I would follow To Fix this without downtime — none holding an exclusive lock for long:
1. Add the column nullable, no unique, no default. Metadata-only on Postgres 11+, so
   effectively instant. Set lock_timeout = '3s' before any DDL so a blocked ALTER fails
   fast instead of queueing the whole app behind it, then retry.

2. Deploy code that writes content_hash on every create/update, so the backfill only
   has to deal with history.

3. Backfill in batches by pk — as a management command, not RunPython. A 500k-row loop
   inside `migrate` blocks the deploy and cannot be paused, resumed or throttled.
   Then check for genuine duplicates before indexing
   (values('content_hash').annotate(n=Count('id')).filter(n__gt=1)); a collision makes
   CREATE UNIQUE INDEX CONCURRENTLY leave behind an INVALID index you must drop first.

4. Build the unique index CONCURRENTLY, then SET NOT NULL. Django has no ORM operation
   for a concurrent *unique* index, so it is RunSQL inside SeparateDatabaseAndState
   with atomic = False (CONCURRENTLY cannot run in a transaction block). And a naive
   SET NOT NULL full-scans under ACCESS EXCLUSIVE — on Postgres 12+ you add a NOT VALID
   check, validate it without an exclusive lock, then SET NOT NULL proves instantly
   from it.
"""

#FIX:
# Migration 2 — the only non-obvious one.
class Migration(migrations.Migration):
    atomic = False  # CREATE INDEX CONCURRENTLY cannot run inside a transaction
    dependencies = [('post', '0060_post_content_hash_nullable')]
    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
                        "post_post_content_hash_uniq ON post_post (content_hash);"
                    ),
                    reverse_sql=(
                        "DROP INDEX CONCURRENTLY IF EXISTS post_post_content_hash_uniq;"
                    ),
                ),
            ],
            state_operations=[
                migrations.AddConstraint(
                    model_name='post',
                    constraint=models.UniqueConstraint(
                        fields=['content_hash'],
                        name='post_post_content_hash_uniq',
                    ),
                ),
            ],
        ),
    ]

# Migration 3 — NOT NULL without a scan under an exclusive lock (Postgres 12+).
class Migration(migrations.Migration):
    atomic = False
    dependencies = [('post', '0061_post_content_hash_unique')]
    operations = [
        migrations.RunSQL(
            "ALTER TABLE post_post ADD CONSTRAINT post_content_hash_not_null "
            "CHECK (content_hash IS NOT NULL) NOT VALID;",
            reverse_sql="ALTER TABLE post_post DROP CONSTRAINT post_content_hash_not_null;",
        ),
        migrations.RunSQL(
            "ALTER TABLE post_post VALIDATE CONSTRAINT post_content_hash_not_null;",
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            "ALTER TABLE post_post ALTER COLUMN content_hash SET NOT NULL;",
            reverse_sql="ALTER TABLE post_post ALTER COLUMN content_hash DROP NOT NULL;",
        ),
        migrations.RunSQL(
            "ALTER TABLE post_post DROP CONSTRAINT post_content_hash_not_null;",
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]



# Section 2 — Real Decisions  [40 pts]
# These questions have no single right answer. We are looking for how you think and what trade-offs you understand. Be direct and specific.

# Q4  Celery task design   [10 pts]
# You need to send push notifications to all followers of a user when they publish a post. A popular creator has 50,000 followers. Walk through exactly how you would design this Celery task. What goes wrong with a naive implementation and how do you prevent it?

Q4_ANSWER = """
Q4 — The naive version — one task looping followers.all() and calling send_push — could go wrong in
several ways such as:
  - It occupies one worker slot for 30–60 minutes. Password resets and payment
    callbacks queue behind it; one popular creator starves the system.
  - It loads 50,000 rows into memory, and each send is a separate HTTPS round trip.
    The round trips are the real cost, not the query.
  - It is not idempotent. Die at follower 40,000, retry, and the first 40,000 get the
    push twice. Users notice immediately.
  - Passing the follower list as a task argument puts megabytes into the broker.
  - No FCM rate awareness, so Firebase throttles the project.

How I would design it to prevent the above issues:

1. The request only enqueues an id, via transaction.on_commit(). Without on_commit the
   worker can pick the message up before the row is committed — the exact DoesNotExist
   from Q1. Never put model instances or 50k lists in the broker.

2. A coordinator task that fans out and never sends. It walks tokens with
   .values_list('token', flat=True).iterator(chunk_size=2000) so memory stays flat at
   any follower count, filters is_active and notification preferences in SQL rather
   than Python, and dispatches one child task per 500 tokens.

3. 500 is FCM's multicast cap, so each child is one HTTP call, not 500. That is 100
   small tasks spread across every worker, and a failure re-sends 500 pushes rather
   than 50,000.

4. An idempotency key — a unique (post_id, user_id) row, or Redis SETNX on
   f'push:{post_id}:{batch_no}'. Retries are guaranteed to happen eventually, and
   without a dedupe key they are user-visible.

5. Its own queue and worker pool, so a fan-out can never delay a payment webhook.
   rate_limit to stay inside the FCM quota, and expires=3600 so a push stuck in a
   backlog is dropped instead of firing at 3am.

6. Per-token handling of the FCM response array: UNREGISTERED means the app was
   uninstalled, so mark the token dead and stop paying for it; 429/5xx retries with
   exponential backoff plus jitter. Jitter is not optional — without it all 100 batches
   retry at the same instant and recreate the spike that failed you.

7. acks_late + reject_on_worker_lost so a batch on a SIGKILLed worker is redelivered,
   and soft_time_limit so a hung HTTPS call cannot pin a worker slot.

From my experience, working at Humdov: an early version of a bulk push/SMS path I worked on ran the sends on
a threading.Thread spawned from the request. It looks fine in staging and is quietly
wrong — the thread dies with the gunicorn worker on the next deploy, with no retry, no
visibility and no backpressure. Coordinator plus per-batch child tasks is the fix.
"""



# Q5  Database index decision   [10 pts]
# You have a SupportTicket table with 200,000 rows. The admin dashboard runs this query hundreds of times per hour:

SupportTicket.objects.filter(
    status='open',
    assigned_operator=request.user
).order_by('-created_at')[:20]

# Explain exactly what index or indexes you would add, why, and what the EXPLAIN output would look like before and after. If you would not add an index, explain why.

Q5_ANSWER = """
Q5 — I would add One composite index:

    models.Index(fields=['assigned_operator', 'status', '-created_at'],
                 name='ticket_operator_status_created_idx')

Column order is the whole answer: equality predicates first, sort column last.
assigned_operator leads because it is far more selective — hundreds of operators
against maybe five statuses; leading with status means walking a fifth of the table.
created_at trailing is what removes the sort. Postgres walks the index in reverse and
stops after 20 entries, so LIMIT 20 is genuinely cheap instead of "sort 3,000 rows and
discard 2,980".

What the output would like Before:
Django auto-indexes the FK, so the planner is not helpless, but it can only use
that index for one of the two predicates:

    Limit  (actual time=38.9..38.9 rows=20)
      ->  Sort   Sort Key: created_at DESC
            Sort Method: top-N heapsort  Memory: 42kB
            ->  Bitmap Heap Scan on support_ticket  (actual rows=2974)
                  Recheck Cond: (assigned_operator_id = 42)
                  Filter: ((status)::text = 'open'::text)
                  Rows Removed by Filter: 5102
    Execution Time: 39.2 ms

The two tells are 'Rows Removed by Filter: 5102' — 5,000 heap rows read and thrown
away — and 'top-N heapsort', meaning the sort is happening at runtime instead of coming
free from an index.


What the output would like After:
    Limit  (actual time=0.031..0.078 rows=20)
      ->  Index Scan using ticket_operator_status_created_idx on support_ticket
            Index Cond: ((assigned_operator_id = 42) AND ((status)::text = 'open'))
    Execution Time: 0.11 ms

No Sort node, nothing discarded, cost bounded by the LIMIT rather than by the size of
the operator's ticket history. At hundreds of calls an hour the bigger win is not the
40ms — it is no longer evicting the working set from cache 5,000 heap pages at a time.

Worth considering: if 'open' is a small slice of the 200k, a partial index
(condition=Q(status='open')) gives the same plan in a much smaller index that closed
tickets never bloat. Trade-off is it only serves the open tab.

Two things I would check before shipping: build it CONCURRENTLY (same reasoning as Q3),
and confirm the dashboard is not also running an unbounded .count() for pagination —
that is a full index scan per page load and would dominate the timing regardless.

Cost is one extra index on a table taking a few thousand writes a day against hundreds
of reads an hour. Easy trade.
"""



# Q6  Debugging a production spike   [10 pts]
# Your EC2 server CPU spiked to 91% at 4am and the Celery worker was killed with SIGKILL (signal 9). You have access to: CloudWatch (CPU only), Celery logs, and the Django codebase. Walk through exactly how you would diagnose what caused this. What are the first three things you check and why?

Q6_ANSWER = """
Q6 — Diagnosing the 4am spike

The thing that reframes the whole investigation: signal 9 is not something the app or
Celery does to itself — Celery stops workers with SIGTERM. SIGKILL on a loaded box is
almost always the kernel OOM killer. So my hypothesis before I look at anything is
memory, not CPU. The 91% is the symptom: a process ballooning into swap thrashes, and
the graph goes vertical while the machine does no useful work. CloudWatch giving me CPU
only is exactly why this looks confusing — the metric that explains it is the one I do
not have.

The First three things I would check:

1. Confirm it was the OOM killer, on the box.
       sudo dmesg -T | grep -i -E 'killed process|out of memory'
   The kernel line gives the PID, the process and its anon-rss at death, which
   immediately separates "one task allocated 3GB" from "eight prefork children at 400MB
   each on a 4GB box". If there is no OOM line my hypothesis is wrong and I pivot — a
   deploy, a systemd restart, an ASG termination in CloudTrail. This is first because it
   decides whether the next two questions are even the right ones.

2. Find what was running. In the Celery logs, the last 'Task received' lines with no
   matching 'succeeded'/'failed' are what was in flight. 4am is a strong hint on its
   own, so in parallel I grep beat_schedule, crontab -l and EventBridge rules. In my
   experience the 4am killer is a nightly report, export or third-party sync — the jobs
   written to "just process everything" and never revisited after the table grew.

3. Read that task for unbounded memory. Specifically: .all() or a wide filter()
   materialised into a list instead of .iterator(); building a CSV/XLSX in memory before
   writing; accumulating a paginated API response; prefetch_related over a huge
   queryset; DEBUG=True in prod, which grows connection.queries forever in a
   long-running process. Then concurrency against RAM — `-c 8` children at a few hundred
   MB each will OOM a small instance even when no single task is unreasonable. That is a
   config bug, not a code bug.

To confirm rather than guess: re-run the suspect in staging against prod-sized data
under /usr/bin/time -v and watch peak RSS, and check queue depth around 04:00 — a
backlog means memory is the product of task size and how many ran at once.

Fixes afterwards:
  - worker_max_tasks_per_child and worker_max_memory_per_child, so a leaky child
    recycles instead of taking the instance down with it.
  - acks_late + reject_on_worker_lost. Right now the in-flight task is simply gone —
    SIGKILL means no except block, no Sentry event, no retry. Same silent-failure class
    as Q1. Plus soft_time_limit so a hung task raises rather than holding a slot.
  - Rewrite the job to stream with .values_list().iterator() and chunk into child
    tasks, so peak memory tracks chunk size, not table size.
  - Move the worker off the gunicorn box, or lower concurrency. The OOM killer picks
    the largest RSS, so next time it may take the web process instead.
  - The fix that matters most for next time: CloudWatch agent publishing
    mem_used_percent and swap, alarming on those and on worker restart count.
    Diagnosing an OOM from a CPU-only dashboard is guesswork, and I would rather not
    do it twice.
"""



# Q7  Security review   [10 pts]
# Review this endpoint and list every security issue you can find. For each one, state the risk and the fix.

@api_view(['POST'])
@permission_classes([AllowAny])
def reset_password(request):
    email = request.data.get('email')
    try:
        user = User.objects.get(email=email)
        token = str(random.randint(1000, 9999))
        user.reset_token = token
        user.save()
        send_reset_email.delay(email, token)
        return Response({'detail': 'Email sent'})
    except User.DoesNotExist:
        return Response({'detail': 'No account found'}, status=404)

Q7_ANSWER = """
Q7 — Security issues I could find:

1. User enumeration. 404 for unknown emails, 200 for known ones.
   Risk: feed in a breach list and learn exactly who has an account here — a phishing
   target list, and a privacy issue in its own right.
   Fix: one generic 200 on both paths, doing the same work either way so timing does
   not leak the answer instead.

2. 4-digit token from `random`. Two separate failures: randint(1000, 9999) is 9,000
   values, brute-forced in seconds with no lockout; and `random` is Mersenne Twister,
   not a CSPRNG, so a few observed outputs reconstruct its state and predict every
   future token — an attacker only needs resets on their own account.
   Fix: secrets.token_urlsafe(32), or Django's default_token_generator (signed, bound
   to the password hash, self-expiring).

3. No expiry and no single-use. reset_token sits on the row until overwritten.
   Risk: an email leaked, forwarded, or in an abandoned mailbox is a permanent account
   takeover credential.
   Fix: store reset_token_created_at, reject past ~15 minutes, null the token on use.

4. Token stored in plaintext.
   Risk: a stolen backup, a leaked dump, SQL injection elsewhere, or an
   over-permissioned ops account is takeover on everyone with a pending reset. A reset
   token is a password equivalent and should never be at rest in the clear.
   Fix: store sha256(token) and compare digests.

5. No rate limiting on an AllowAny endpoint.
   Risk: enumerate the whole user table; mail-bomb one user; burn the SES quota and the
   domain's sending reputation so genuine mail stops being delivered; and with a 4-digit
   token, trigger a reset then walk all 9,000 codes on the verify endpoint.
   Fix: DRF throttle keyed on IP *and* the submitted email — one alone just gets
   rotated — plus an attempt counter that kills the token after ~5 wrong guesses.

6. No input validation.
   Risk: request.data.get('email') can be None, a list or a dict — at
   best no match, at worst a 500. Unnormalised case also means 'Foo@x.com' fails to
   find 'foo@x.com', which users read as their account being gone.
   Fix: serializer with EmailField, lowercase it, look up with iexact.

7. get() can raise MultipleObjectsReturned — Django's default User does not enforce a
   unique email.
   Risk: an uncaught 500, which is itself an enumeration oracle.
   Fix: .filter().first(), and a unique constraint on the model.

8. user.save() writes every column.
   Risk: clobbers a concurrent write to that row, or the token is silently lost —
   producing "the code never works" reports nobody can reproduce.
   Fix: save(update_fields=[...]).

9. The plaintext token crosses the broker.
   Risk: .delay(email, token) puts it in Redis, the
   result backend, Flower's task list and any Sentry breadcrumb capturing task args —
   three systems nobody treats as a secret store.
   Fix: pass the user id only, and let the task mint, hash, store and send it.

10. No audit logging.
    Risk: An enumeration sweep leaves no trace until a user complains.
    Fix: log masked email, IP and user agent at INFO. Never the token.
"""

#FIX:
import hashlib
import logging
import secrets
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework import serializers, status
from rest_framework.decorators import api_view, permission_classes, throttle_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.throttling import ScopedRateThrottle

logger = logging.getLogger(__name__)
User = get_user_model()

RESET_TOKEN_TTL = timedelta(minutes=15)

# Identical on every path so the response body never reveals whether the account exists.
GENERIC_RESET_RESPONSE = {
    'detail': 'If an account exists for that email, we have sent reset instructions.'
}

# settings.py:
#   REST_FRAMEWORK = {'DEFAULT_THROTTLE_RATES': {'password_reset': '5/hour'}}

class PasswordResetRequestSerializer(serializers.Serializer):
    email = serializers.EmailField()

    def validate_email(self, value):
        return value.strip().lower()

class PasswordResetThrottle(ScopedRateThrottle):
    scope = 'password_reset'

@api_view(['POST'])
@permission_classes([AllowAny])
@throttle_classes([PasswordResetThrottle])
def reset_password(request):
    serializer = PasswordResetRequestSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    email = serializer.validated_data['email']

    # .filter().first() — no DoesNotExist, no MultipleObjectsReturned, no 500 oracle.
    user = User.objects.filter(email__iexact=email).order_by('pk').first()

    if user is not None:
        # Only the id crosses the broker. The task mints and stores the token.
        send_reset_email.delay(user.pk)

    logger.info(
        'password_reset requested email=%s ip=%s ua=%s found=%s',
        _mask_email(email),
        request.META.get('REMOTE_ADDR'),
        request.META.get('HTTP_USER_AGENT', '')[:120],
        user is not None,
    )
    return Response(GENERIC_RESET_RESPONSE, status=status.HTTP_200_OK)


@shared_task
def send_reset_email(user_id):
    user = User.objects.filter(pk=user_id).first()
    if user is None:
        return

    raw_token = secrets.token_urlsafe(32)
    User.objects.filter(pk=user.pk).update(
        reset_token=hashlib.sha256(raw_token.encode()).hexdigest(),
        reset_token_created_at=timezone.now(),
        reset_token_attempts=0,
    )
    deliver_email(user.email, build_reset_url(user.pk, raw_token))  # never log raw_token




# Section 3 — Write It  [30 pts]
# Q8 — Django management command  [30 pts]
# Write a Django management command called audit_stale_rewards. It must:
# 1.  Find all Reward rows where status='claimed' and claimed_at is more than 7 days ago
# 2.  Print a summary: how many found, broken down by reward_type
# 3.  Accept a --fix flag that marks them as 'expired' and sets an expires_at timestamp
# 4.  Without --fix, it must be a completely safe dry run — nothing changes in the DB
# 5.  Log each expired reward ID at INFO level using Python's logging module, not print()
# The Reward model has at minimum: id, status (CharField), claimed_at (DateTimeField), expires_at (DateTimeField, nullable), reward_type (CharField).
# 📌 Do not use Django admin or third-party packages. Standard library + Django only. We will read your code for clarity, correctness, and Django conventions — not just whether it runs.

Q8_ANSWER = """
Q8 — Notes on the implementation below

File: rewards/management/commands/audit_stale_rewards.py

- The dry run issues SELECTs only. No transaction, no .save(), no .update() anywhere on
  that path — the --fix branch is the only code that can write. I made that structural
  rather than a `if not fix:` guard inside a write loop, because that is how dry runs
  stop being safe.

- The breakdown is aggregated in SQL with .values().annotate(Count()), not by counting
  rows in Python — one query instead of 200,000 objects in memory.

- Writes are batched .update() keyed on pk, not a save() loop: one statement per batch,
  no signal storm, and no transaction long enough to hurt a live table.

- The batch is locked with select_for_update() and the UPDATE re-asserts
  status='claimed'. Without it, a reward approved in the seconds between the SELECT and
  the UPDATE gets silently expired out from under a supervisor — the same race as Q2.
  The lock is also what makes the logged IDs match the rows actually changed, so the log
  is usable as an audit trail.

- self.stdout for the operator watching the terminal, logger.info for whoever has to
  reconstruct this three weeks later. print() serves neither: it bypasses log routing,
  ignores --no-color, and cannot be captured by call_command(stdout=...) in a test.

- --days defaults to 7 so the spec holds, but as an argument the command is testable
  without fabricating week-old fixtures.

- Idempotent by construction: expired rows no longer match the filter, so re-running is
  a no-op. Safe to schedule.
"""

#ANSWER:
# rewards/management/commands/audit_stale_rewards.py
"""Audit rewards left in 'claimed' status past their expiry window.

Read-only by default. Pass --fix to expire the rows it finds.

    python manage.py audit_stale_rewards            # dry run
    python manage.py audit_stale_rewards --fix      # expire them
    python manage.py audit_stale_rewards --days 30  # widen the window
"""
import logging
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from rewards.models import Reward

logger = logging.getLogger(__name__)

CLAIMED = "claimed"
EXPIRED = "expired"

DEFAULT_STALE_DAYS = 7
BATCH_SIZE = 500

class Command(BaseCommand):
    help = (
        "Report rewards still in 'claimed' status more than N days (default 7) after "
        "they were claimed. Read-only unless --fix is passed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--fix",
            action="store_true",
            help="Mark the stale rewards as 'expired' and stamp expires_at. "
                 "Without this flag the command only reports.",
        )
        parser.add_argument(
            "--days",
            type=int,
            default=DEFAULT_STALE_DAYS,
            help=f"Age in days after which a claimed reward is stale "
                 f"(default: {DEFAULT_STALE_DAYS}).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=BATCH_SIZE,
            help=f"Rows to expire per transaction (default: {BATCH_SIZE}).",
        )

    def handle(self, *args, **options):
        days = options["days"]
        batch_size = options["batch_size"]

        if days < 1:
            raise CommandError("--days must be at least 1.")
        if batch_size < 1:
            raise CommandError("--batch-size must be at least 1.")

        cutoff = timezone.now() - timedelta(days=days)
        stale = Reward.objects.filter(status=CLAIMED, claimed_at__lt=cutoff)

        total = self._report(stale, cutoff, days)
        if not total:
            return "No stale rewards found."

        if not options["fix"]:
            self.stdout.write(
                self.style.WARNING(
                    "\nDry run — nothing was written. Re-run with --fix to expire these."
                )
            )
            return f"{total} stale reward(s) found."

        expired = self._expire(stale, batch_size)
        self.stdout.write(self.style.SUCCESS(f"\nExpired {expired} reward(s)."))

        skipped = total - expired
        if skipped:
            # Rows that changed status between the report and the update.
            self.stdout.write(
                self.style.WARNING(
                    f"{skipped} reward(s) were modified during the run and left alone."
                )
            )
        return f"Expired {expired} of {total} stale reward(s)."

    def _report(self, stale, cutoff, days):
        """Print the summary. Returns the total found. Read-only."""
        breakdown = (
            stale.values("reward_type")
            .annotate(count=Count("id"))
            .order_by("-count", "reward_type")
        )
        rows = list(breakdown)
        total = sum(row["count"] for row in rows)

        self.stdout.write(
            f"Rewards still 'claimed' after {days} day(s) "
            f"(claimed before {cutoff:%Y-%m-%d %H:%M %Z}): {total}"
        )
        if not rows:
            return 0

        width = max(len(row["reward_type"] or "(unset)") for row in rows)
        self.stdout.write("\nBy reward_type:")
        for row in rows:
            label = row["reward_type"] or "(unset)"
            self.stdout.write(f"  {label:<{width}}  {row['count']:>6}")

        return total

    def _expire(self, stale, batch_size):
        """Expire the stale rows in batches. Returns the number actually changed."""
        expired_at = timezone.now()
        total = 0

        while True:
            with transaction.atomic():
                # Lock the batch so nothing can approve these rows between the read
                # and the write, and so the IDs logged are exactly the rows changed.
                ids = list(
                    stale.select_for_update()
                    .order_by("pk")
                    .values_list("pk", flat=True)[:batch_size]
                )
                if not ids:
                    break

                # status=CLAIMED is re-asserted defensively; the lock already
                # guarantees it, but it makes the query safe to read in isolation.
                changed = Reward.objects.filter(pk__in=ids, status=CLAIMED).update(
                    status=EXPIRED,
                    expires_at=expired_at,
                )

            for reward_id in ids:
                logger.info(
                    "Expired stale reward id=%s (claimed status held past cutoff)",
                    reward_id,
                )

            total += changed
            self.stdout.write(f"  ...expired {total} so far", ending="\r")

        return total
