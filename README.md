# gcloud-download.py
```
Usage: ./gcloud-download.py gs://<bucket>/mydir /some/path
optional arguments:
        --parallel Concurrent async download.
        -h or --help This output.

```

### Normal Invocation:
```
./gcloud-download.py gs://<bucket>/mydir ab
```
#### Copies sequentially like
```
"mydir" folder structure is as follows:
mydir/
mydir/a/
mydir/a/1.txt
mydir/a/b/
mydir/a/b/2.txt
```
#### Resultant folder structure.
```
$pwd/ab/mydir/
$pwd/ab/mydir/a/
$pwd/ab/mydir/a/1.txt
$pwd/ab/mydir/a/b/
$pwd/ab/mydir/a/b/2.txt
```

### `--concur` Invocation:
#### * Calls async/await to create tasks and downlaod.
#### * It however uses a sync function to create directories to avoid race condition.
