# varda2-client

## Usage

```
usage: varda2-client [-h] [-s SERVER] [-c CERTIFICATE]
                     {submit,monitor,save,version,annotate,stab,seq,snv,mnv,task,sample}
                     ...

positional arguments:
  {submit,monitor,save,version,annotate,stab,seq,snv,mnv,task,sample}
    submit              Submit without upload
    monitor             Monitor tasks
    save                Save tables
    version             Retrieve version
    annotate            Annotate file(s) with optional upload
    stab                Stab query
    seq                 Sequence query
    snv                 SNV query
    mnv                 MNV query
    task                Task query
    sample              Sample query

optional arguments:
  -h, --help            show this help message and exit
  -s SERVER, --server SERVER
                        Server hostname
  -c CERTIFICATE, --certificate CERTIFICATE
                        Certificate
```
