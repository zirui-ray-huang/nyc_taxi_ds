import os
import asyncio
import streamlit as st
import nest_asyncio
import io
from contextlib import AsyncExitStack, redirect_stdout
from dotenv import load_dotenv

# MCP and LangChain
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_classic.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate

# 1. CRITICAL: Allow nested loops for Streamlit compatibility
nest_asyncio.apply()
load_dotenv()

# --- Configuration ---
MD_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
GOOGLE_KEY = os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

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

@st.cache_resource
def get_mcp_connection():
    """Initializes the connection ONCE and caches it."""
    # Create a persistent loop for this resource
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    stack = AsyncExitStack()
    
    # 2. CRITICAL: Silencing stdout to prevent JSON-RPC corruption
    f = io.StringIO()
    with redirect_stdout(f):
        read, write = loop.run_until_complete(stack.enter_async_context(stdio_client(server_params)))
        session = loop.run_until_complete(stack.enter_async_context(ClientSession(read, write)))
        loop.run_until_complete(session.initialize())
        tools = loop.run_until_complete(load_mcp_tools(session))
    
    # Return exactly 4 items
    return session, tools, stack, loop

def run_chat():
    st.set_page_config(page_title="MotherDuck AI", layout="wide")
    st.title("🦆 MotherDuck AI Data Assistant")

    # 3. FIX: Unpack exactly 4 items
    try:
        session, tools, stack, loop = get_mcp_connection()
    except Exception as e:
        st.error(f"Failed to connect to MotherDuck: {e}")
        return

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if user_query := st.chat_input("Ask about nyc_taxi_db..."):
        st.session_state.messages.append({"role": "user", "content": user_query})
        with st.chat_message("user"):
            st.markdown(user_query)

        with st.chat_message("assistant"):
            with st.spinner("Processing..."):
                try:
                    llm = ChatGoogleGenerativeAI(model=GEMINI_MODEL, google_api_key=GOOGLE_KEY, temperature=0)
                    prompt = ChatPromptTemplate.from_messages([
                        ("system", (
                            "You are a professional SQL analyst for the 'nyc_taxi_db' database in MotherDuck. "
                            "IMPORTANT RULES:\n"
                            "1. For USER TABLES (like trips, rides, drivers), ALWAYS use the prefix: 'nyc_taxi_db.tablename'.\n"
                            "   For SYSTEM TABLES (information_schema), DO NOT add a prefix. Example: `SELECT * FROM information_schema.tables WHERE table_catalog = 'nyc_taxi_db'`.\n"
                            "2. Use DuckDB SQL dialect.\n"
                            "3. To list tables, use: 'SHOW TABLES;'\n"
                            "4. To see table structure, use: 'DESCRIBE table_name;'\n"
                            "5. Always prefix tables with 'nyc_taxi_db.' (e.g., nyc_taxi_db.rides).\n"
                            "6. If you encounter a 'Table does not exist' error, use 'SHOW TABLES' to verify the name."
                        )),
                        ("placeholder", "{chat_history}"),
                        ("human", "{input}"),
                        ("placeholder", "{agent_scratchpad}"),
                    ])
                    
                    # 4. CRITICAL: verbose=False prevents 'Thinking' hangs
                    agent = create_tool_calling_agent(llm, tools, prompt)
                    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=False)

                    # Execute in the cached loop, silencing any stray logs
                    f = io.StringIO()
                    with redirect_stdout(f):
                        response = loop.run_until_complete(agent_executor.ainvoke({"input": user_query}))
                    
                    raw_output = response["output"]

                    if isinstance(raw_output, list):
                        # Extract 'text' from the MCP content blocks
                        clean_text = ""
                        for part in raw_output:
                            if isinstance(part, dict) and 'text' in part:
                                clean_text += part['text']
                            elif isinstance(part, str):
                                clean_text += part
                        final_output = clean_text.strip()
                    else:
                        final_output = str(raw_output)
                    
                    st.markdown(final_output)
                    st.session_state.messages.append({"role": "assistant", "content": final_output})

                except Exception as e:
                    st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    run_chat()