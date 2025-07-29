## Doc build instructions

First build the library (in the root of the repo)

```bash
pip install build
python -m build
```

At that point the build package should be in `./dist`
which you can install with

```
pip install ./dist/
```

Then run install the necessary packages 
```
cd docs
pip install sphinx
pip install sphinxcontrib-napoleon
pip install sphinx_rtd_theme
pip install sphinx_copybutton
```

to build the documentation simply run (from the docs folder)

```
cd docs
make html
```

## Editing documentation
You can edit the documentation from the `source` directory.

