# Agent Quick Ops

## Restart Screen processes

From `/var/www/Wb-review`:

```bash
screen -S wb-main -X quit || true
screen -S wb-admin -X quit || true
screen -S wb-main -dm bash -lc 'source venv/bin/activate && python main.py'
screen -S wb-admin -dm bash -lc 'source venv/bin/activate && python admin.py'
```

Optional log check:

```bash
screen -S wb-main -X hardcopy -h /tmp/wb-main.screen.log || true
sed -n '1,120p' /tmp/wb-main.screen.log
```
