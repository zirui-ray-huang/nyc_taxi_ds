import os
import asyncio
import threading
import streamlit as st
from dotenv import load_dotenv

# MCP and LangChain imports
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_classic.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

# Load variables from .env
load_dotenv()

# Retrieve credentials
MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")

# Configuration for the MotherDuck MCP Server
server_params = StdioServerParameters(
    command="uvx",
    args=[
        "mcp-server-motherduck",
        "--db-path", "md:nyc_taxi_db",
        "--motherduck-token", MD_TOKEN,
        "--read-write"
    ],
    env={"MOTHERDUCK_TOKEN": MD_TOKEN}
)

async def process_query(user_query):
    """Handles the async MCP connection and Agent execution."""
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # Bridge MCP tools to LangChain
            tools = await load_mcp_tools(session)
            
            # Setup Gemini (Using 1.5 Pro)
            llm = ChatGoogleGenerativeAI(
                model="gemini-2.5-flash",
                google_api_key=GOOGLE_KEY,
                temperature=0
            )
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", "You are a professional SQL analyst for the 'nyc_taxi_db' database."
                " IMPORTANT: You must ONLY query and discuss tables within the 'nyc_taxi_db' database."
                " Ignore the 'sample_data' or 'system' databases entirely." 
                " Always prefix your table names with 'nyc_taxi_db.' to ensure you are using the correct context."
                " Use MotherDuck tools to query data and answer questions accurately."),
                ("placeholder", "{chat_history}"),
                ("human", "{input}"),
                ("placeholder", "{agent_scratchpad}"),
            ])

            agent = create_tool_calling_agent(llm, tools, prompt)
            agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
            
            # Run the agent
            response = await agent_executor.ainvoke({"input": user_query})
            raw_output = response["output"]

            # --- CLEANING LOGIC ---
            # If output is a list (MCP artifact format), extract just the text parts
            if isinstance(raw_output, list):
                clean_text = ""
                for part in raw_output:
                    if isinstance(part, dict) and 'text' in part:
                        clean_text += part['text']
                    elif isinstance(part, str):
                        clean_text += part
                return clean_text.strip()
            
            # If it's already a string, just return it
            return str(raw_output)

def run_chat():
    st.set_page_config(page_title="MotherDuck x Gemini", layout="wide")
    st.title("🦆 MotherDuck AI Data Assistant")

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display existing chat
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Error handling for missing credentials
    if not MD_TOKEN or not GOOGLE_KEY:
        st.error("Missing MOTHERDUCK_TOKEN or GOOGLE_API_KEY in .env file.")
        return

    # Chat UI logic
    if user_query := st.chat_input("Ex: Show me the top 5 records from green_taxi"):
        # 1. Add user message to state and UI
        st.session_state.messages.append({"role": "user", "content": user_query})
        with st.chat_message("user"):
            st.markdown(user_query)

        # 2. Process Assistant Response
        with st.chat_message("assistant"):
            with st.spinner("Querying MotherDuck..."):
                try:
                    # To avoid the 'missing ScriptRunContext' error, 
                    # we ensure the async loop runs within the Streamlit context.
                    output = asyncio.run(process_query(user_query))
                    
                    st.markdown(output)
                    st.session_state.messages.append({"role": "assistant", "content": output})
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    run_chat()