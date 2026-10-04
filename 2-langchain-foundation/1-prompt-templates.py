from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()  # take environment variables from .env.

llm_gemini = ChatGoogleGenerativeAI(model="gemini-3.8-flash")

prompt = ChatPromptTemplate.from_messages(
    [  
        ("system","You are an expert educational content creator."),
        ("human","""
    Create a detailed learning outline on the topic of
            {topic} for {audience}.
            Respond with format of:
            Topic Title:<Title>
            Content Outline:<list of bullet points>
         """)

    ]
)

# first step - build the prompt with variables,
# second step - invoke the LLM with the built prompt

chain = prompt | llm_gemini # create a chain by piping prompt to llm(LCEL syntax)

variables = {
"topic": "NVIDIA Certification of Agentic AI",
"audience": "aspiring AI professionals"

}

response = chain.invoke(variables)
print("Response from Gemini Model:")
print("Response Content:")
print(response.text)