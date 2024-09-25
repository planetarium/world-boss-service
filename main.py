import sys

import uvicorn

from world_boss.app.scheduler import scheduler

if __name__ == "__main__":
    workers = int(sys.argv[1])
    timeout_keep_alive = int(sys.argv[2])
    host = sys.argv[3]
    port = int(sys.argv[4])
    scheduler.start()
    uvicorn.run(
        "world_boss.wsgi:app",
        workers=workers,
        timeout_keep_alive=timeout_keep_alive,
        host=host,
        port=port,
    )
