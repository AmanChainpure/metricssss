import datetime
from azure.cosmos import CosmosClient
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
import pandas as pd


query = """
        SELECT  *
        FROM c
        WHERE c.createdAt >= '2025-04-01'
        ORDER BY c.updatedAt DESC
        """


parameters = []
chat_history_logs = list(cosmos_container.query_items(
            query=query,
            parameters=parameters, 
            enable_cross_partition_query=True
        ))      



def feedback(row):
    if row['thumpsUp'] == 0.0 and row['thumpsDown']==0.0:
        val = "no-feedback"
    elif row['thumpsUp'] == 1.0 and row['thumpsDown']==0.0:
        val = "thumpsUp"
    elif row['thumpsUp'] == 0.0 and row['thumpsDown']==1.0:
        val = "thumpsDown"
    else:
        val = "no-feedback"
    return val


df_data = pd.DataFrame(chat_history_logs)
df_data.head()



def month_week(dates):
    firstday_month = dates - pd.to_timedelta(dates.dt.day - 1, unit='d')
    return (dates.dt.day-1 + firstday_month.dt.weekday) // 7 + 1

df_data['created_date'] = pd.to_datetime(df_data['createdAt'], format='mixed').dt.date
df_data['month'] = pd.to_datetime(df_data['createdAt'], format='mixed').dt.month
df_data['week_of_month'] = month_week(pd.to_datetime(df_data['createdAt'], format='mixed'))
df_data['month-week'] = df_data['month'].astype(str) + '-' + df_data['week_of_month'].astype(str)

df_data['feedback'] = df_data.apply(feedback, axis=1)

df_data[['input_token_count','output_tokens','total_tokens','responseTime', 'thumpsUp','thumpsDown']] = df_data[['input_token_count','output_tokens','total_tokens','responseTime', 'thumpsUp','thumpsDown']].fillna(0.0)

df_data_flat = df_data[['userId', 'id', 'type','created_date','month','week_of_month','month-week','user_query', 'output_response',
                        'chat_id', 'input_token_count','output_tokens','total_tokens','responseTime', 'thumpsUp','thumpsDown','feedback']]
df_data_flat.head()



import matplotlib.pyplot as plt
#df_data_flat[['month-week','feedback']]
df_grouped = df_data_flat.groupby('month-week')['feedback'].value_counts().unstack(fill_value=0)
# Create stacked bar plot
df_grouped.plot(kind='bar', stacked=True)
plt.title('Feedback by Month-Week')
plt.xlabel('Month-Week')
plt.ylabel('Count')
plt.show()



# df_data_flat.groupby(['month-week','feedback'])va.plot(kind='bar', stacked=True, figsize=(10, 6))
# plt.xlabel('Category')
# plt.ylabel('Values')
# plt.title('Stacked Bar Plot')
# plt.legend(title='Values')
# Displaying the plot
plt.show()



pd.pivot_table(df_data_flat.groupby(['month-week','feedback']).size().reset_index(name='count').sort_values(by=['month-week','feedback']),
                            index='month-week', 
                            columns='feedback', 
                            values=['count'],
                            aggfunc=np.sum).fillna(0)




df_data_flat.groupby(['month','week_of_month']).agg(messages_count=('user_query', 'size'),average_response_time = ('responseTime','mean')).reset_index()




bins = [1,10,30,60,10000]
df_data_flat.groupby(['month-week', pd.cut(df_data_flat.responseTime, bins)]).size().unstack()

bins = [1,10,30,60,10000]
ax = df_data_flat.groupby(['month-week', pd.cut(df_data_flat.responseTime, bins)]).size().unstack().plot(kind='bar')
for p in ax.patches:
    ax.annotate(str(p.get_height()), (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='bottom')
plt.title('Avg-Response time (Weekly)')
plt.xlabel('Month-Week')
plt.ylabel('Query counts')
plt.legend([' 0-10sec','10-30sec','30-60sec','60+ sec'])
plt.show()




df_data_flat.groupby(['month-week']).agg(inputTokens = ('input_token_count','sum'),
                                        outputTokens = ('output_tokens','sum'),
                                        totalTokens = ('total_tokens','sum'),
                                        ).reset_index()
df_weekly_users = df_data_flat.groupby(['month','week_of_month','userId']).size().reset_index(name='query_count')
df_weekly_users[df_weekly_users['query_count']>=3].groupby(['month','week_of_month']).agg(active_users=('userId', 'size'),
    active_users_list=('userId', lambda x: list(x.unique()))
).reset_index()
df_data_flat.groupby(['month','week_of_month']).agg(messages_count=('user_query', 'size'),average_response_time = ('responseTime','mean')).reset_index()



query = """
        SELECT  
                VALUE COUNT(1)
        FROM c
        WHERE ARRAY_LENGTH(c.messages) > 0 AND c.createdAt > '2025-04-01'
        ORDER BY c.updatedAt DESC
        """
parameters = []
chat_history_o1 = list(cosmos_container.query_items(
            query=query,
            parameters=parameters, 
            enable_cross_partition_query=True
        ))        

df_data_o1 = pd.DataFrame(chat_history_o1)
df_data_o1.head()

