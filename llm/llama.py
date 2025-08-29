import torch
from .llm import LLM
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline, BitsAndBytesConfig


class Llama(LLM):
    def __init__(self, path, name, load_in_8bit=False):
        super().__init__("Meta-Llama-3.1-8B-Instruct", path, name)

        if load_in_8bit:
            bnb_config = BitsAndBytesConfig(load_in_8bit=True)
            model_kwargs = {"quantization_config": bnb_config}
        else:
            model_kwargs = {"torch_dtype": torch.bfloat16}
        
        # 加载 LLM
        self.model = AutoModelForCausalLM.from_pretrained(path, device_map="cuda", **model_kwargs).eval()
        self.tokenizer = AutoTokenizer.from_pretrained(path, device_map="cuda")
        self.tokenizer.pad_token_id = self.tokenizer.eos_token_id

    def infer(self, input_text: str, system_prompt: str | None = None, **kwargs) -> str:
        if system_prompt is None:
            messages = [
                {"role": "user", "content": input_text}
            ]
        else:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": input_text}
            ]

        inputs = self.tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
            return_dict=True
        ).to(self.model.device) # type: ignore

        input_len = inputs['input_ids'].shape[1] # type: ignore

        generate_kwargs = {
            "input_ids": inputs['input_ids'],
            "attention_mask": inputs['attention_mask'],
            "pad_token_id": self.tokenizer.eos_token_id,
            **kwargs
        }

        out = self.model.generate(**generate_kwargs)
        answer = self.tokenizer.decode(out[0][input_len:], skip_special_tokens=True)
        return answer
    
    def infer_multiple(self, messages, n, **kwargs) -> str:
        inputs = self.tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            # tokenize=True,
            return_tensors="pt",
            return_dict=True
        ).to(self.model.device) # type: ignore

        input_len = inputs['input_ids'].shape[1] # type: ignore

        generate_kwargs = {
            "input_ids": inputs['input_ids'],
            "attention_mask": inputs['attention_mask'],
            "num_return_sequences": n,
            "pad_token_id": self.tokenizer.eos_token_id,
            **kwargs
        }

        out = self.model.generate(**generate_kwargs)
        answer = [self.tokenizer.decode(out[i][input_len:], skip_special_tokens=True) for i in range(n)]
        return answer

