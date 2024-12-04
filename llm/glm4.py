import torch
from .llm import LLM
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModelForCausalLM # type: ignore


class GLM4(LLM):
    def __init__(self, path, name, peft=None, int4=False):
        super().__init__("glm-4-9b-chat-hf", path, name)
        
        # 加载 LLM
        self.tokenizer = AutoTokenizer.from_pretrained(path, device_map="cuda")

        if int4:
            self.model = AutoModelForCausalLM.from_pretrained(path, device_map="cuda", load_in_4bit=True)
        else:
            self.model = AutoModelForCausalLM.from_pretrained(path, device_map="cuda", torch_dtype=torch.bfloat16)
        
        if peft:
            self.model = PeftModelForCausalLM.from_pretrained(
                model=self.model,
                model_id=peft,
                trust_remote_code=True
            )
        
        self.model = self.model.eval()

    def infer(self, input_text, system_prompt, **kwargs) -> str:
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
            # tokenize=True,
            return_tensors="pt",
            return_dict=True
        ).to(self.model.device) # type: ignore

        input_len = inputs['input_ids'].shape[1] # type: ignore

        generate_kwargs = {
            "input_ids": inputs['input_ids'],
            "attention_mask": inputs['attention_mask'],
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
            **kwargs
        }

        out = self.model.generate(**generate_kwargs)
        answer = [self.tokenizer.decode(out[i][input_len:], skip_special_tokens=True) for i in range(n)]
        return answer
