import json
import os
from langchain import OpenAI, SQLDatabase, SQLDatabaseChain
from langchain.chat_models import ChatOpenAI
from langchain.prompts.prompt import PromptTemplate

# Set your OpenAI API key as an environment variable in the Lambda function configuration
# os.environ["OPENAI_API_KEY"] = "your-api-key-here"

def lambda_handler(event, context):
    # Extract parameters from the event
    db_uri = event['db_uri']
    question = event['question']
    dialect = event['dialect']

    # Connect to the SQL database
    db = SQLDatabase.from_uri(db_uri)

    # Initialize the language model
    llm = ChatOpenAI(temperature=0)

    # Define the prompt template
    _DEFAULT_TEMPLATE = """Given an input question, first create a syntactically correct {dialect} query to run, then look at the results of the query and return the answer.
    Use the following format:
    Question: "Question here"
    SQLQuery: "SQL Query to run"
    SQLResult: "Result of the SQLQuery"
    Answer: "Final answer here"
    Only use the following tables:
    {table_info}
    If someone asks for the table foobar, respond that there is no table named foobar.
    Question: {input}"""

    PROMPT = PromptTemplate(
        input_variables=["input", "dialect", "table_info"],
        template=_DEFAULT_TEMPLATE
    )

    # Create the SQL database chain
    db_chain = SQLDatabaseChain.from_llm(llm, db, prompt=PROMPT, verbose=True)

    # Get table info
    table_info = db.get_table_info()

    # Run the query
    result = db_chain.run(input=question, dialect=dialect, table_info=table_info)

    # Return the result
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }