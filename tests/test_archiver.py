import asyncio
import os
import sys

# Add the parent directory to Python path to find the src module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import archiver as av
from src.archiver import BlueskyArchiver


# ---------------------------------------------------------------------------
# Stall watchdog
# ---------------------------------------------------------------------------

class _FakeWS:
    """Minimal stand-in for a websockets connection the watchdog may close."""

    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


async def _drain(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_watchdog_closes_socket_on_stall():
    """No messages past STALL_RESTART_S → watchdog closes the wedged socket."""
    async def inner():
        a = BlueskyArchiver(archive_all=True)
        a.running = True
        loop = asyncio.get_event_loop()
        ws = _FakeWS()
        a._ws = ws
        a._last_msg_monotonic = loop.time() - 1.0  # "long ago" vs tiny threshold

        orig = (av.WATCHDOG_TICK_S, av.STALL_WARN_S, av.STALL_RESTART_S)
        av.WATCHDOG_TICK_S, av.STALL_WARN_S, av.STALL_RESTART_S = 0.01, 0.2, 0.5
        try:
            wd = asyncio.create_task(a._watchdog())
            for _ in range(200):  # up to ~2s for the watchdog to fire
                if ws.closed:
                    break
                await asyncio.sleep(0.01)
            a.running = False
            await _drain(wd)
        finally:
            av.WATCHDOG_TICK_S, av.STALL_WARN_S, av.STALL_RESTART_S = orig

        assert ws.closed is True

    asyncio.run(inner())


def test_watchdog_leaves_live_socket_alone():
    """While messages keep arriving the watchdog must never close the socket."""
    async def inner():
        a = BlueskyArchiver(archive_all=True)
        a.running = True
        loop = asyncio.get_event_loop()
        ws = _FakeWS()
        a._ws = ws

        orig = (av.WATCHDOG_TICK_S, av.STALL_WARN_S, av.STALL_RESTART_S)
        av.WATCHDOG_TICK_S, av.STALL_WARN_S, av.STALL_RESTART_S = 0.01, 0.2, 0.5
        try:
            wd = asyncio.create_task(a._watchdog())
            for _ in range(80):  # ~0.8s of steady "traffic", > STALL_RESTART_S
                a._last_msg_monotonic = loop.time()
                await asyncio.sleep(0.01)
            a.running = False
            await _drain(wd)
        finally:
            av.WATCHDOG_TICK_S, av.STALL_WARN_S, av.STALL_RESTART_S = orig

        assert ws.closed is False

    asyncio.run(inner())

async def test_stream_posts():
    """Test that stream_posts yields valid post records."""
    print("\nTesting stream_posts functionality...")
    archiver = BlueskyArchiver()
    posts_received = 0
    required_fields = {'handle', 'time_us', 'record', 'rkey'}
    
    try:
        async for post in archiver.stream_posts():
            # Verify post structure
            if not isinstance(post, dict):
                raise AssertionError("Post should be a dictionary")
            
            # Check required fields
            missing_fields = required_fields - post.keys()
            if missing_fields:
                raise AssertionError(
                    f"Post missing required fields: {missing_fields}"
                )
            
            # Verify record structure
            if 'text' not in post['record']:
                raise AssertionError("Post record should contain text")
            if 'createdAt' not in post['record']:
                raise AssertionError("Post record should contain createdAt")
            
            # Verify handle structure
            if 'handle' not in post:
                raise AssertionError("Post should contain handle")
            
            posts_received += 1
            print(f"✓ Received post {posts_received}: {post['record']['text'][:100]}...")
            
            if posts_received >= 3:  # Test with first 3 posts
                archiver.stop()
                break
                
    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        archiver.stop()
        raise e
    
    if posts_received == 0:
        raise AssertionError("Should receive at least one post")
    
    print(f"✓ Successfully received and validated {posts_received} posts")
    return True

def run_tests():
    """Run all tests."""
    try:
        asyncio.run(test_stream_posts())
        print("\n✓ All tests passed!")
    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        raise e

if __name__ == "__main__":
    run_tests()