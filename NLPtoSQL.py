#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
import openai


# In[2]:


df =pd.read_csv('sales_data_sample.csv')


# In[ ]:


df.groupby('QTR_ID').sum()['SALES']


# In[3]:


import sqlalchemy


# In[4]:


from sqlalchemy import create_engine
from sqlalchemy import text


# In[5]:


temp_data =  create_engine('sqlite:///:memory:', echo=True)


# In[ ]:


temp_data


# In[6]:


data = df.to_sql(name='Sales', con=temp_data)


# In[ ]:


with temp_data.connect() as connection:
    result = connection.execute(text ('SELECT SUM(SALES) from Sales'))
    


# In[ ]:


with temp_data.connect() as conn:
    res = conn.execute(text('SELECT * from Sales'))


# In[7]:


df.columns


# In[8]:


def table_definition(df):
    
    prompt = '''SQL table with its properties:Sales({})'''.format(','.join([str(col) for col in df.columns]))
    return prompt


# In[9]:


table_definition(df)


# In[10]:


def prompt_input():
    nlp_text = input('Enter query')
    return nlp_text


# In[ ]:


prompt_input()


# In[11]:


def combine_prompts(df,query_prompt):
    definition = table_definition(df)
    query_init_string = f'\n###A query: {query_prompt}\nSELECT'
    return definition + query_init_string
    


# In[52]:


nlp_text = prompt_input()
prompt = combine_prompts(df, nlp_text)


# In[53]:


prompt


# In[ ]:


os.getcwd()


# In[14]:


f = open('C:\\Users\\ndang\\Desktop\\openai.txt')


# In[15]:


os.environ['OPENAI_KEY'] = f.read()


# In[16]:


openai.api_key = os.getenv('OPENAI_KEY')


# In[62]:


openai_output = openai.Completion.create(
                         model = 'text-davinci-003',
                         prompt = combine_prompts(df, nlp_text),
                         temperature = 0,
                         max_tokens = 150,
                         top_p = 1,
                         frequency_penalty = 0,
                         presence_penalty = 0,
                         stop = [';','#']
)


# In[63]:


openai_output['choices'][0]['text']


# In[64]:


def handle_output(response):
    query = openai_output['choices'][0]['text']
    if query.startswith(" "):
        return 'SELECT' + query
    else :
        return query


# In[65]:


handle_output(openai_output)


# In[72]:


with temp_data.connect() as conn:
    result = conn.execute(text(handle_output(openai_output)))


# In[70]:


print(f'RESULT:\n{result.all()}')


# In[73]:


for i in result.all():
    print(i)


# In[ ]:




