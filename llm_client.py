import os
from transformers import pipeline, AutoTokenizer, AutoModelForCausalLM
import torch

# 👇 Classe de base abstraite
class LLMClient:
    def ask(self, prompt: str) -> str:
        raise NotImplementedError("Méthode 'ask' non implémentée.")


class LocalLLMClient(LLMClient):
    def __init__(self):
        model_path = "ETL_IA/Mistral"  # ou "ETL_IA/Mistral"

        os.makedirs("offload", exist_ok=True)

        tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            local_files_only=True
        )

        model = AutoModelForCausalLM.from_pretrained(
            model_path,
            torch_dtype=torch.float16,
            device_map="auto",
            offload_folder="offload",
            local_files_only=True
        )

        self.pipeline = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer
        )

    def ask(self, prompt: str) -> str:
        return self.pipeline(prompt, max_new_tokens=512)[0]["generated_text"]


def get_llm_client(source="local") -> LLMClient:
    return LocalLLMClient()
