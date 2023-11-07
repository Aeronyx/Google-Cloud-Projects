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

connector.close()



""" MODEL CODE """
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn import metrics

# sliced to remove first four IPs that i made up by hand for testing
clients = df.iloc[4:]

# remove periods in IPs
def cut(x):
    return x.replace('.','')

# preprocessing
clients.loc[:, 'client_ip'] = clients['client_ip'].apply(cut)
clients['country'] = clients['country'].astype('category')
clients.loc[:, 'country'] = clients['country'].cat.codes

# feature and target
X_1 = clients[['client_ip']]
y_1 = clients['country']

# train/test split
X_train_1, X_test_1, y_train_1, y_test_1 = train_test_split(X_1, y_1, test_size=0.3)

# predict
tree = DecisionTreeClassifier()
tree = tree.fit(X_train_1,y_train_1)
y_pred = tree.predict(X_test_1)



""" LOGGING CODE """
message = "Prediction Accuracy (Country by IP, Decision Tree):" + str(metrics.accuracy_score(y_test_1, y_pred))
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