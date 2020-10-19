from kapacitor.udf.agent import Agent, Handler
from kapacitor.udf import udf_pb2
import logging
import json

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s', filename='/var/spool/kapacitor_udf_log.log')
logger = logging.getLogger()


class SimpleUDF(Handler):
    """ 
        This UDF consumes batch data from TICK script and writes it back to stream 
    """
        
    def __init__(self, agent):
        logger.info("__Init__ ")
        self.agent = agent
        self.requested_ncpus = 0
        self.used_nodes = 0
        self.node_request_efficiency = 0

    def info(self):
        logger.info("Info")
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.STREAM
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        return response

    def init(self, init_req):
        success = True
        logger.info("Init")
        response = udf_pb2.Response()
        response.init.success = success
        return response

    def begin_batch(self, begin_req):
        logger.info("Begining batch")

    def snapshot(self):
        response = udf_pb2.Response()
        response.snapshot.snapshot=json.dumps({"snapshot":"lets worry later"}).encode()
        return response

    def restore(self, restore_req):
        response = udf_pb2.Response()
        response.restore.success = False
        response.restore.error = 'not implemented'
        return response
            
    def point(self, point):
        logger.info("In point")

        response = udf_pb2.Response()
        response.point.name = point.name
        response.point.tags.update(point.tags)
        response.point.time = point.time
        response.point.group = point.group
        response.point.fieldsInt.update(point.fieldsInt)
        response.point.fieldsDouble.update(point.fieldsDouble)

        logger.info("FieldInt: ")
        for key, value in response.point.fieldsInt.items():
             loger.info("{}: {}".format(key, value))
        self.agent.write_response(response)


    def end_batch(self, batch_meta):
        logging.info("end batch")


if __name__ == "__main__":
    agent = Agent()
    h = SimpleUDF(agent)
    agent.handler = h
    agent.start()
    agent.wait()
