# SPS-SQL: Enhancing Text-to-SQL Generation on Small-Scale LLMs with Pre-Synthesized Queries 

## Requirements

```bash
pip install -r requirements.txt
```

Please note that `requiurements.txt` may be incomplete. Install missing packages manually in need.

## Customizing LLMs

All LLMs are implementing abstract class `llm.LLM`. We implemented GLM-4, Llama 3.1 and Qwen 2.5 in folder `llm`. You can write your own LLM class besides them.

## Templating

```bash
python template.py \
    --sql-json [Path to Spider's train dataset json] \
    --table-json [Path to Spider's table json] \
    --output [Where to output templates (single pkl file)] \
    --limit [How many templates should be reserved]
```

## Synthesis

```bash
python generate.py \
    --table-json [Path to Spider's table json] \
    --db-names [DB names used, sep by space] \
    --db-dir [Path to the directory containing databases] \
    --templates [Templates pkl file] \
    --output [Where to write] \
    --maximum-sqls-per-template 256 \
    --template-limit 256 \
```

## Narrate

You can change the LLM used by modifying the LLM-calling part in `narrate.py`.

```bash
python narrate.py \
    --input [Synthed SQLs] \
    --table [Path to Spider's table json] \
    --base-model [Model path] \
    --output-result [Where to write narrated pairs] \
```

## Pooling

```bash
python build_pool.py \
    --bge-model [BGE model path] \
    --result-files [Path to narrated pairs] \
    --output-dataset-prefix [Output prefix of pools] \
```

## Inferring

You can change the LLM used by modifying the LLM-calling part in `infer.py`, and prompts can be replaced by choosing another Jinja2 template.

```bash
python infer.py \
    --dataset-type spider \
    --test-set [Path to Spider's dev/test set] \
    --table [Path to Spider's table json] \
    --reference-datasets-prefix [Prefix of pools] \
    --reference-shot [Shot, for zero-shot use 0] \
    --base-model [LLM path] \
    --bge-model [BGE model path] \
    --output-result [Where to write result] \
    [--sl \]
    [--sl-model [Path to Schema Linking model]]
```

## Test Accuracy

```bash
python validate.py --result-file [Result] --db-dir [Path to database dir] [--start 0 --end 200]
```

## License

MIT

[test-suite-sql-eval](https://github.com/taoyds/test-suite-sql-eval/) is licensed under Apache