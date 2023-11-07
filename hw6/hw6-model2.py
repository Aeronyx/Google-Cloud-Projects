# George Trammell

""" DATABASE CODE """
from google.cloud.sql.connector import Connector
from google.cloud import logging, pubsub_v1
import sqlalchemy
import pandas as pd

db_connection_string = 'robust-builder-398218:us-east1:ds-561-hw5-mysql'
db_user = 'root'
db_password ='VGX]_SzED@v3&:[K'
db_database = 'hw5database'

connector = Connector()

def getconn():
    conn = connector.connect(
        db_connection_string,
        "pymysql",
        user=db_user,
        password=db_password,
        db=db_database
    )
    return conn

pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn
)

with pool.connect() as connection:
    result = connection.execute(
        sqlalchemy.text(
            "SELECT * FROM clients;"
        )
    ).fetchall()

COLS = ['id', 'client_ip', 'country', 'gender', 'age', 'income', 'banned']
df = pd.DataFrame(result, columns=COLS)
df.set_index('id', drop=True, inplace=True)

connector.close()



""" MODEL CODE """
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics

# preprocessing

# sliced to remove first four IPs that i made up by hand for testing
clients = df.iloc[4:]

# client_ip encoding
def cut(x):
    return x.replace('.','')
clients.loc[:, 'client_ip'] = clients['client_ip'].apply(cut)
clients['client_ip'] = clients['client_ip'].astype('int64')

# country encoding
clients['country'] = clients['country'].astype('category')
clients.loc[:, 'country'] = clients['country'].cat.codes

# gender encoding
clients['gender'] = clients['gender'].astype('category')
clients.loc[:, 'gender'] = clients['gender'].cat.codes

# age encoding
clients['age'] = clients['age'].astype('category')
clients.loc[:, 'age'] = clients['age'].cat.codes

# income encoding
clients['income'] = clients['income'].astype('category')
clients.loc[:, 'income'] = clients['income'].cat.codes

# feature and target
X_1 = clients[['client_ip', 'country', 'gender', 'age', 'banned']]
y_1 = clients['income']

# train/test split
X_train_1, X_test_1, y_train_1, y_test_1 = train_test_split(X_1, y_1, test_size=0.30)

# prediction
tree = DecisionTreeClassifier()
tree = tree.fit(X_train_1,y_train_1)
y_pred = tree.predict(X_test_1)

forest = RandomForestClassifier()
forest = forest.fit(X_train_1,y_train_1)
y_pred2 = forest.predict(X_test_1)



""" LOGGING CODE """
half1 = "Prediction Accuracy (Income by All, Decision Tree):" + str(metrics.accuracy_score(y_test_1, y_pred))
half2 = "Prediction Accuracy (Income by All, Random Forest):" + str(metrics.accuracy_score(y_test_1, y_pred2))
message = half1 + half2

project_id = 'robust-builder-398218'
logger_name = 'ds561-logging'

logging_client = logging.Client(project=project_id)
logger = logging_client.logger(logger_name)

logger.log_text(message)

# pubsub
topic_name = 'ds561'
pub_client = pubsub_v1.PublisherClient()
topic_path = pub_client.topic_path(project_id, topic_name)

try:
    data = message.encode('utf-8')
    future = pub_client.publish(topic_path, data)
    message_id = future.result()
    logger.log(f"Message published with ID: {message_id}")
except Exception as e:
    logger.log(f"PubSub Notification Failed: {str(e)}")