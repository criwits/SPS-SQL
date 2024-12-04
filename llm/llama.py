import torch
from .llm import LLM
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline


class Llama(LLM):
    def __init__(self, path, name):
        super().__init__("Meta-Llama-3.1-8B-Instruct", path, name)
        
        # 加载 LLM
        self.pipe = pipeline(
            "text-generation",
            model=path,
            max_new_tokens=256,
            model_kwargs={"torch_dtype": torch.bfloat16},
            device="cuda",
        )

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
        answer = self.pipe(messages,
                           num_return_sequences=1, pad_token_id=self.pipe.tokenizer.eos_token_id, **kwargs)[0]["generated_text"][-1]["content"] # type: ignore
        return answer # type: ignore
    
    def infer_multiple(self, messages, n, **kwargs) -> str:
        answer = [self.pipe(messages,
                           num_return_sequences=n, pad_token_id=self.pipe.tokenizer.eos_token_id, **kwargs)[i]["generated_text"][-1]["content"] for i in range(n)] #type: ignore
        return answer # type: ignore