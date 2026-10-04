from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()  # take environment variables from .env.

llm_gemini = ChatGoogleGenerativeAI(model="gemini-3.8-flash")

message = "What is best wy to learn  Nvidia Certification of Agentic AI"
response_gemini = llm_gemini.invoke(message)

print("Response from Gemini Model:")
print(response_gemini)
print("Response Content:")
print(response_gemini.content)

