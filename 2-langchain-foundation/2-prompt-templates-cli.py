from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
import argparse

load_dotenv()  # take environment variables from .env.

llm_gemini = ChatGoogleGenerativeAI(model="gemini-3.8-flash")

args = argparse.ArgumentParser(description = "Generate learning outline using Gemini LLM")
args.add_argument("--topic",type=str,required=True,help="Topic for learning outline")
args.add_argument("--audience",type=str,required=True,help="Target audience for the learning outline")
parsed_args = args.parse_args()
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
"topic": parsed_args.topic,
"audience": parsed_args.audience

}

response = chain.invoke(variables)
print("Response from Gemini Model:")
print("Response Content:")
print(response.text)

# python 2-prompt-templates-cli.py --topic "Agentic AI" --audience "aspiring AI Professionals"
# python 2-prompt-templates-cli.py --topic "Agentic AI" --audience "Test Engineers"