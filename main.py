from langgraph.graph import StateGraph
from langchain_openai import ChatOpenAI
from src.langgraph_redis_checkpointer import AsyncRedisSaver
from langgraph.graph.message import MessagesState
from redis.asyncio import Redis

conn = Redis()

memory = AsyncRedisSaver(conn)


class State(MessagesState):
    ...


graph_builder = StateGraph(State)
llm = ChatOpenAI(model='gemini-2.5-flash',
                 base_url='https://generativelanguage.googleapis.com/v1beta/openai/', api_key='AIzaSyBE32DU...')


async def chat_node(state: State):
    result = await llm.ainvoke(state['messages'])
    return {'messages': [result]}


graph_builder.add_node(chat_node)
graph_builder.set_entry_point('chat_node')

graph = graph_builder.compile(
    checkpointer=memory
)


async def main():
    res = await graph.ainvoke({'messages': [{'role': 'user', 'content': '你好，我是周代琳，你介绍下你自己'}]}, config={
        'configurable': {
            'thread_id': '142314'
        }
    })

    print(res)

    res = await graph.ainvoke({'messages': [{'role': 'user', 'content': '我叫啥名字'}]}, config={
        'configurable': {
            'thread_id': '142314'
        }
    })

    print(res)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
