"""Admin services dispatcher."""
import asyncio
import sys
import json
from datetime import datetime, timezone


def _log(level, event, **kw):
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                      "level": level, "event": event, **kw}), flush=True)


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""

    if name == "watchdog":
        from app.admin.watchdog import main
    elif name == "admin_bot":
        from app.admin.admin_bot import main
    else:
        _log("error", "unknown_service", name=name)
        sys.exit(1)

    asyncio.run(main())
