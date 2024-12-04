from .llm import LLM
from transformers import AutoModelForCausalLM, AutoTokenizer

class Qwen(LLM):
    def __init__(self, path, name):
        super().__init__("qwen", path, name)

        self.model = AutoModelForCausalLM.from_pretrained(
            path,
            torch_dtype="auto",
            device_map="auto"
        )

        self.tokenizer = AutoTokenizer.from_pretrained(path)


    def infer(self, input_text, system_prompt, **kwargs) -> str:
        if system_prompt is None:
            msgs = [
                {"role": "user", "content": input_text}
            ]
        else:
            msgs = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": input_text}
            ]
        text = self.tokenizer.apply_chat_template(
            msgs,
            tokenize=False,
            add_generation_prompt=True
        )
        model_inputs = self.tokenizer([text], return_tensors="pt").to(self.model.device) # type: ignore

        generated_ids = self.model.generate(
            **model_inputs,
            **kwargs
        )
        generated_ids = [
            output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
        ]
        response = self.tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
        return response
