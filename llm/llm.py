from abc import ABC


class LLM(ABC):
    def __init__(self, model: str, model_path: str, name: str) -> None:
        self.model = model
        self.model_path = model_path
        self.name = name

    def infer(self, input_text: str, system_prompt: str | None = None, **kwargs) -> str:
        return NotImplemented
    

llm_instance: LLM | None = None

def get_llm_instance() -> LLM:
    global llm_instance
    if llm_instance is None:
        raise Exception("LLM instance is not initialized.")
    return llm_instance

def load_llm_instance(llm: LLM) -> None:
    global llm_instance
    llm_instance = llm