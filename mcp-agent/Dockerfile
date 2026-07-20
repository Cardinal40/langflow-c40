FROM python:3.12-slim
RUN pip install --no-cache-dir "mcp>=1.2.0" "httpx>=0.27"
WORKDIR /app
COPY langflow_agent.py .
ENV MCP_TRANSPORT=http MCP_HOST=0.0.0.0 MCP_PORT=8765
# Set at run time: LANGFLOW_URL, LANGFLOW_API_KEY, FLOW_ID, MCP_PATH_SECRET
EXPOSE 8765
CMD ["python", "langflow_agent.py"]
