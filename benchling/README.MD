
# To use.

## Init program

```
./init.sh
source ./__benchling__/bin/activate
./install.sh
```

## Setup your environment vars from template
```
cp _.env-example .env
```

## Edit values as you see fit

```
./run.sh
```

## Advanced examples

Below are a couple of advanced ways to run the crawler

### Indexing with verbosity, a rate of .1 requests a second, and starting with the 10th record
```
./run.sh -v -r .1 --min-idx 10 
```

### Indexing with verbosity and a specific ID in Benchling
```
./run.sh -v -i 823737
```

### Indexing just publicly available docs (uses 'testing' user for pages query)
```
./run.sh -v -p
```