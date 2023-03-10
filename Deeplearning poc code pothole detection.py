# Databricks notebook source
dbutils.fs.rm('dbfs:/FileStore/train/pothole', True)

# COMMAND ----------

pip install tensorflow==2.11.*

# COMMAND ----------

import tensorflow as tf
from tensorflow import keras
from keras import Sequential
from keras.layers import Dense,Conv2D,MaxPooling2D,Flatten,BatchNormalization,Dropout

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://pothole-poc-dataset@zee.blob.core.windows.net",
  mount_point = "/mnt/iotdata",
  extra_configs = {"fs.azure.account.key.zee.blob.core.windows.net":"YlBjJI7gIJpTUQ5ELXbqiqI/T/WhLRDi03zTBEiVU30O0O9zXwkWqKULPst5k+5cors7NYTbyw/v+AStJ3bIBw=="})

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/iotdata/train")

# COMMAND ----------

# pothole and normal data path pre processing

normal_train_path = '/dbfs/mnt/iotdata/train/normal'

pothole_train_path = '/dbfs/mnt/iotdata/train/pothole'

normal_test_path = '/dbfs/mnt/iotdata/test/normal'

pothole_test_path = '/dbfs/mnt/iotdata/test/pothole'

# COMMAND ----------

# data_preP dir


normal_train_path_final = '/dbfs/mnt/iotdata/right-formate-data/train/zeent'

pothole_train_path_final = '/dbfs/mnt/iotdata/right-formate-data/train/zeept'

normal_test_path_final = '/dbfs/mnt/iotdata/right-formate-data/test/zeenv'

pothole_test_path_final = '/dbfs/mnt/iotdata/right-formate-data/test/zeepv'

# COMMAND ----------

pip install Pillow

# COMMAND ----------

import os

# COMMAND ----------

from PIL import Image

for i in os.listdir(normal_train_path):
  dir = normal_train_path + '/' + i
  im = Image.open(dir)
  rgb_im = im.convert('RGB')
  rgb_im.save(normal_train_path_final + '/' + i)

# COMMAND ----------

from PIL import Image

for i in os.listdir(pothole_train_path):
  dir = pothole_train_path + '/' + i
  im = Image.open(dir)
  rgb_im = im.convert('RGB')
  rgb_im.save(pothole_train_path_final + '/' + i)

# COMMAND ----------

from PIL import Image

for i in os.listdir(normal_test_path):
  dir = normal_test_path + '/' + i
  im = Image.open(dir)
  rgb_im = im.convert('RGB')
  rgb_im.save(normal_test_path_final + '/' + i)

# COMMAND ----------

from PIL import Image

for i in os.listdir(pothole_test_path):
  dir = pothole_test_path + '/' + i
  im = Image.open(dir)
  rgb_im = im.convert('RGB')
  rgb_im.save(pothole_test_path_final + '/' + i)

# COMMAND ----------



# COMMAND ----------

# from PIL import Image
# import os

# list_path =[normal_train_path, pothole_train_path, normal_test_path, pothole_test_path]
# final_path = [normal_train_path_final, pothole_train_path_final, normal_test_path_final, pothole_test_path_final]

# for j in list_path:
#   for k in final_path:
#     for i in os.listdir(j):
#       dir = j + '/' + i
#       im = Image.open(dir)
#       rgb_im = im.convert('RGB')
#       rgb_im.save(k + '/' + i )


# COMMAND ----------

list_path =[normal_train_path, pothole_train_path, normal_test_path, pothole_test_path]
final_path = [normal_train_path_final, pothole_train_path_final, normal_test_path_final, pothole_test_path_final]

# COMMAND ----------

from PIL import Image

for i,j in enumerate(list_path):
    for k in os.listdir(j):
      dir = j + '/' + k
      im = Image.open(dir)
      rgb_im = im.convert('RGB')
      rgb_im.save(final_path[i] + '/' + k )

# COMMAND ----------

import shutil

# COMMAND ----------

shutil.rmtree('/dbfs/mnt/iotdata/right-formate-data/train/normal')

# COMMAND ----------



# COMMAND ----------

sa = dbutils.fs.ls(normal_train_path)

# COMMAND ----------

from PIL import Image
import os

# COMMAND ----------

 for i in sa:
        im = Image.open(i)
        C = im.convert('RGB')
        print(c)
        break

# COMMAND ----------

Image.open('dbfs:/mnt/iotdata/train/normal/100.jpg')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# generators
from tensorflow import keras

train_ds = keras.utils.image_dataset_from_directory(
    directory = '/dbfs/mnt/iotdata/right-formate-data/train',
    labels='inferred',
    label_mode = 'int',
    batch_size=32,
    image_size=(256,256)
)

validation_ds = keras.utils.image_dataset_from_directory(
    directory = '/dbfs/mnt/iotdata/right-formate-data/test',
    labels='inferred',
    label_mode = 'int',
    batch_size=32,
    image_size=(256,256)
)

# COMMAND ----------

# Normalize
import tensorflow as tf
def process(image,label):
    image = tf.cast(image/255. ,tf.float32)
    return image,label

train_ds = train_ds.map(process)
validation_ds = validation_ds.map(process)

# COMMAND ----------

# create CNN model
from keras import Sequential
from keras.layers import Dense,Conv2D,MaxPooling2D,Flatten,BatchNormalization,Dropout


model = Sequential()

model.add(Conv2D(32,kernel_size=(3,3),padding='valid',activation='relu',input_shape=(256,256,3)))
model.add(BatchNormalization())
model.add(MaxPooling2D(pool_size=(2,2),strides=2,padding='valid'))

model.add(Conv2D(64,kernel_size=(3,3),padding='valid',activation='relu'))
model.add(BatchNormalization())
model.add(MaxPooling2D(pool_size=(2,2),strides=2,padding='valid'))

model.add(Conv2D(128,kernel_size=(3,3),padding='valid',activation='relu'))
model.add(BatchNormalization())
model.add(MaxPooling2D(pool_size=(2,2),strides=2,padding='valid'))

model.add(Flatten())

model.add(Dense(128,activation='relu'))
model.add(Dropout(0.1))
model.add(Dense(64,activation='relu'))
model.add(Dropout(0.1))
model.add(Dense(1,activation='sigmoid'))

# COMMAND ----------

from keras import Sequential
from keras.layers import Dense,Conv2D,MaxPooling2D,Flatten,BatchNormalization,Dropout


model.summary()

# COMMAND ----------

model.summary()

# COMMAND ----------

model.compile(optimizer='adam',loss='binary_crossentropy',metrics=['accuracy'])

# COMMAND ----------

history = model.fit(train_ds,epochs=10,validation_data=validation_ds)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.plot(history.history['accuracy'],color='red',label='train')
plt.plot(history.history['val_accuracy'],color='blue',label='validation')
plt.legend()
plt.show()

# COMMAND ----------

plt.plot(history.history['loss'],color='red',label='train')
plt.plot(history.history['val_loss'],color='blue',label='validation')
plt.legend()
plt.show()

# COMMAND ----------

pip install opencv-python

# COMMAND ----------

import tensorflow
from tensorflow import keras
from keras import Sequential
from keras.layers import Dense,Flatten
from keras.applications.vgg16 import VGG16

# COMMAND ----------

conv_base = VGG16(
    weights='imagenet',
    include_top = False,
    input_shape=(256,256,3)
)

# COMMAND ----------

conv_base.summary()

# COMMAND ----------

model_1 = Sequential()

model_1.add(conv_base)
model_1.add(Flatten())
model_1.add(Dense(256,activation='relu'))
model_1.add(Dense(1,activation='sigmoid'))

# COMMAND ----------

model_1.summary()

# COMMAND ----------

conv_base.trainable = False

# COMMAND ----------

model_1.compile(optimizer='adam',loss='binary_crossentropy',metrics=['accuracy'])

# COMMAND ----------

history_1 = model_1.fit(train_ds,epochs=10,validation_data=validation_ds)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.plot(history_1.history['accuracy'],color='red',label='train')
plt.plot(history_1.history['val_accuracy'],color='blue',label='validation')
plt.legend()
plt.show()

# COMMAND ----------

plt.plot(history_1.history['loss'],color='red',label='train')
plt.plot(history_1.history['val_loss'],color='blue',label='validation')
plt.legend()
plt.show()

# COMMAND ----------

import cv2

# COMMAND ----------

test_img = cv2.imread('/dbfs/mnt/iotdata/right-formate-data/test/normal/39.jpg')

# COMMAND ----------

plt.imshow(test_img)

# COMMAND ----------

test_img.shape

# COMMAND ----------

test_img = cv2.resize(test_img,(256,256))

# COMMAND ----------

test_input = test_img.reshape((1,256,256,3))

# COMMAND ----------

model_1.predict(test_input)

# COMMAND ----------

s= [2,3,4,5,6,7]
v= [4,6,7,8,9,9]

u = [9,8,7,6,5,4]

# COMMAND ----------

for i,j in enumerate(s):
    for k in u:
        print(j*v[i]*k)

# COMMAND ----------

for i in s:
    for j in v:
        for k in u:
            print(i*j*k)
        break


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


