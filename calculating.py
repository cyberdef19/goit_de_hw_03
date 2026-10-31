from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.classic.dataframe import DataFrame

spark = SparkSession.builder.appName('GoIT_DE_HW_03').getOrCreate()
products = spark.read.csv('goit_de_hw_03.py/products.csv', header=True, inferSchema=True)
print('Products')
products.show()
purchases = spark.read.csv('goit_de_hw_03.py/purchases.csv', header=True, inferSchema=True)
print('Purchases')
purchases.show()
users = spark.read.csv('goit_de_hw_03.py/users.csv', header=True, inferSchema=True)
print('Users')
users.show()

df = {"Products":products, "Purchases": purchases, "Users": users}

def count_NULL(data: DataFrame) -> dict[str, int]:
    """Повертає кількість NULL""" 
    return {clmn: data.filter(F.col(clmn).isNull()).count() for clmn in data.columns}


print('Кількість NULL значень')
for k, val in df.items():
    print(k)
    print(count_NULL(val))
    
    
#Очистити NULL
products_droppedna = products.dropna()
purchases_droppedna = purchases.dropna()
users_droppedna = users.dropna()

products_droppedna.show()
purchases_droppedna.show()
users_droppedna.show()

#Загальна сума покупок за кожною категорією продуктів

purchases_droppedna.select([F.col('product_id'), F.col('quantity')]).\
                    join(products_droppedna.select([F.col('product_id'), F.col('category'), F.col('price')]), 
                         on="product_id", how='inner').withColumn('total_purchase', F.col('quantity') * F.col('price')).\
                    groupBy('category').sum('total_purchase').show()

#Визначення суми покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.

purchases_droppedna.select([F.col('product_id'), F.col('quantity'), F.col('user_id')]).\
                    join(products_droppedna.select([F.col('product_id'), F.col('category'), F.col('price')]), 
                         on="product_id", how='inner').\
                    join(users_droppedna.select([F.col('user_id'), F.col('name'), F.col('age')]), on='user_id', how='inner').\
                    withColumn('total_sum', F.col('quantity') * F.col('price')).\
                    where(F.col('age') >=18).where(F.col('age') <= 25).groupBy('category').sum('total_sum').show()


#Визначення частки покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років

total_sum = purchases_droppedna.select([F.col('product_id'), F.col('quantity'), F.col('user_id')]).\
                    join(products_droppedna.select([F.col('product_id'), F.col('category'), F.col('price')]), 
                         on="product_id", how='inner').\
                    join(users_droppedna.select([F.col('user_id'), F.col('name'), F.col('age')]), on='user_id', how='inner').\
                    withColumn('total_sum', F.col('quantity') * F.col('price')).\
                    where(F.col('age') >=18).where(F.col('age') <= 25).groupBy('category').sum('total_sum')
               
general_sum = total_sum.select(F.sum("sum(total_sum)")).first().asDict()

part_of_total = total_sum.withColumn("part_of_total_sum", F.round(100*(F.col("sum(total_sum)")/general_sum['sum(sum(total_sum))'])))

#Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років

part_of_total.sort(F.col("part_of_total_sum"), ascending=False).limit(3).show()                             


                    



   


