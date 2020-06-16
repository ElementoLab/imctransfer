# IMCtransfer

A daemon to manage transference of imaging mass cytometry MCD files produced by
the Hyperion instrument of [EIPM](https://eipm.weill.cornell.edu/), from
[WCM's Box.com](https://wcm.app.box.com) to the
[SCU cluster](https://scu.med.cornell.edu/).

It will detect new files added, maintain a database of existing files, download
new files compare hashes and return the metadata of the samples.

In the future it will automatically submit jobs to process the IMC data using
for example [imcpipeline](https://github.com/ElementoLab/imcpipeline).


## Requirements and installation

Requires `Python >= 3.7`.

Install with `pip`:
```bash
pip install git+ssh://git@github.com/elementolab/imctransfer.git
```
While the repository is private, the `git+ssh` protocol requires proper git
configuration.


## Running

Before running it, produce a YAML file with the tokens obtained at the
[WCM Box.com domain](https://wcm.app.box.com/developers/console/app/1288907):
```yaml
client_id: '12345678901234567890123456789012'
client_secret: '12345678901234567890123456789012'
access_token: '12345678901234567890123456789012'
```
Make sure you're the only one who can read the credentials using `chmod 400`.
By default `imctransfer` looks in `~/.box.access_tokens.yaml`, but you can pass
a custom file with the `--secret` option.

Simply run without arguments on the root of the project:
```bash
imctransfer
```

Or specify an alternative root project directory that will be hosting the files,
using the `-o/--output-dir` option:

```bash
imctransfer -o <directory>
```

To see all options, simply do:
```bash
imctransfer --help
```
