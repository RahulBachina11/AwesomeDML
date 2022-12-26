import numpy as np
from keras.preprocessing import image
import tensorflow.compat.v1 as tf

from tensorflow.keras.applications import inception_v3
from tensorflow.keras.applications.inception_v3 import InceptionV3

from tensorflow.keras.applications import resnet50
from tensorflow.keras.applications.resnet50 import ResNet50

import asyncio
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List
import json

# model1 = InceptionV3(weights='imagenet')
# model2 = ResNet50(weights='imagenet')
model1 = None
model2 = None

def run_inference_on_InceptionV3(image_files: list, modelObj = None):

    if modelObj is None:
        modelObj = InceptionV3(weights='imagenet')

    results = {}
    
    for image_file in image_files:

        # print(f"ModelInceptionV3: performing prediction on {image_file}")

        img = tf.keras.utils.load_img(image_file, target_size=(299,299))

        input_img = tf.keras.utils.img_to_array(img)
        input_img = np.expand_dims(input_img, axis=0)
        input_img = inception_v3.preprocess_input(input_img)

        predict_img = modelObj.predict(input_img)

        top_five_predict = inception_v3.decode_predictions(predict_img, top=5)

        results[image_file.split("/")[-1]] = top_five_predict

    return results

def run_inference_on_ResNet50(image_files: list, modelObj = None):

    if modelObj is None:
        modelObj = ResNet50(weights='imagenet')

    results = {}
    
    for image_file in image_files:

        # print(f"ResNet50: performing prediction on {image_file}")

        img = tf.keras.utils.load_img(image_file, target_size=(224, 224))

        input_img = tf.keras.utils.img_to_array(img)
        input_img = np.expand_dims(input_img, axis=0)
        input_img = resnet50.preprocess_input(input_img)

        predict_img = modelObj.predict(input_img)

        top_five_predict = resnet50.decode_predictions(predict_img, top=5)

        results[image_file.split("/")[-1]] = top_five_predict

    return results


async def perform_inference(model_name, files):
    
    function = None
    if model_name == "InceptionV3":
        function = run_inference_on_InceptionV3
    elif model_name == "ResNet50":
        function = run_inference_on_ResNet50
    else:
        return "Invalid model"
    
    with ProcessPoolExecutor() as process_pool:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        call: partial[List[str]] = partial(function, files)
        call_coros = []
        call_coros.append(loop.run_in_executor(process_pool, call))

        results = await asyncio.gather(*call_coros)
        return results[0]

def perform_inference_without_async(model_name, files):

    function = None
    modelObj = None
    if model_name == "InceptionV3":
        function = run_inference_on_InceptionV3
        modelObj = model1
    elif model_name == "ResNet50":
        function = run_inference_on_ResNet50
        modelObj = model2
    else:
        return "Invalid model"
    
    return function(files, modelObj=modelObj)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            # 👇️ alternatively use str()
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

def Merge(dict1, dict2):
    return(dict2.update(dict1))

def dump_to_file(d, filename):
    with open(filename, 'w') as fout:
        json_dumps_str = json.dumps(d, indent=4, cls=NpEncoder)
        print(json_dumps_str, file=fout)

class ModelParameters:

    def __init__(self, download_time, model_load_time, first_image_predict_time, each_image_predict_time, batch_size) -> None:
        
        self.download_time = download_time
        self.model_load_time = model_load_time
        self.first_image_predict_time = first_image_predict_time
        self.each_image_predict_time = each_image_predict_time
        self.batch_size = batch_size
    
    def execution_time_per_vm(self):
        return (self.download_time * self.batch_size) + self.model_load_time + self.first_image_predict_time + (self.each_image_predict_time * (self.batch_size-1))

