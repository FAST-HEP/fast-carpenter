# Examples

## CMS L1T study
```bash
git clone https://github.com/FAST-HEP/fast-carpenter.git
cd fast-carpenter
# if not already installed
pip install -e .
fast_carpenter examples/cms_l1t_data.yml examples/cms_l1t_processing.yml --outdir examples/output/cmsl1t
```

## DUNE Supernova Neutrino Study

```bash
git clone https://github.com/FAST-HEP/fast-carpenter.git
cd fast-carpenter
# if not already installed
pip install -e .
fast_carpenter examples/dune_snnu_data.yml examples/dune_snnu_processing.yml --outdir examples/output/dune_snnu
```