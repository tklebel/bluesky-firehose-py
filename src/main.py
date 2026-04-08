import argparse
import asyncio
import signal
import sys
from archiver import BlueskyArchiver

async def run_archiver(args):
    """Run the archiver with the given arguments."""
    archiver = BlueskyArchiver(
        username=args.username, 
        password=args.password, 
        debug=args.debug,
        stream=args.stream,
        measure_rate=args.measure_rate,
        get_handles=args.get_handles,
        cursor=args.cursor,
        archive_all=args.archive_all,
        archive_non_posts=args.archive_non_posts
    )
    
    def handle_shutdown(sig, frame):
        """Handle shutdown signals."""
        print("\nShutting down gracefully...")
        archiver.stop()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    try:
        await archiver.archive_posts()
    finally:
        archiver.stop()

def main():
    """Main function to run the archiver with signal handling."""
    parser = argparse.ArgumentParser(description='Archive posts from Bluesky firehose')
    parser.add_argument('--username', help='Bluesky username')
    parser.add_argument('--password', help='Bluesky password')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    parser.add_argument('--stream', action='store_true', help='Stream post text to stdout')
    parser.add_argument("--measure-rate", action="store_true", 
                       help="Track and display posts per minute rate")
    parser.add_argument("--get-handles", action="store_true",
                       help="Resolve handles while archiving (significantly slower due to rate limits)")
    parser.add_argument("--cursor", type=int,
                       help="Unix microseconds timestamp to start playback from. Overrides the auto-resume cursor file (default: resume from <data-dir>/.cursor if present)")
    parser.add_argument("--archive-all", action="store_true",
                       help="Archive all records in their original format (not just posts)")
    parser.add_argument("--archive-non-posts", action="store_true",
                       help="Archive everything except posts")
    
    args = parser.parse_args()
    
    if sys.platform == 'win32':
        # Windows specific event loop policy
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(run_archiver(args))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main() 