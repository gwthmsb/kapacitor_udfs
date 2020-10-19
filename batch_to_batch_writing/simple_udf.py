from kapacitor.udf.agent import Agent, Handler
from kapacitor.udf import udf_pb2
import logging
import json, random

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s', filename='kapacitor_udf_log.log')
logger = logging.getLogger()


class SimpleUDF(Handler):
    """ 
        This UDF consume jobs data and find requested to used nodes 
    """
        
    def __init__(self, agent):
        logger.info("__Init__ ")
        self.agent = agent
        self.requested_ncpus = 0
        self.used_nodes = 0
        self.node_request_efficiency = 0
        self.points=[]

    def info(self):
        logger.info("Info")
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH
        #response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        #response.info.options['used_ncpus'].valueTypes.append(udf_pb2.INT)
        return response

    def init(self, init_req):
        success = True
        #for opt in init_req.options:
        #    if opt.name in ['requested_ncpus', 'used_ncpus']:
        #        success = True
        #        break
        logger.info("------------ Init")
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
        logger.info(point.fieldsInt)

        point_res = {}
        point_res['group'] = point.group
        point_res['field1'] = point.fieldsDouble.get('field1')
        point_res['time'] = point.time

        self.points.append(point_res)

    def end_batch(self, end_req):
        logging.info("End batch")
        response = udf_pb2.Response()
        for p in self.points:
            response.point.fieldsDouble['field1'] = random.randrange(1,5)
            response.point.group = p.get("group")
            response.point.time = p.get('time')
            response.point.tags['new_tag'] = 'new_tag_{}'.format(random.randrange(1,5))
            self.agent.write_response(response)

        self.agent.write_response(response)
        # Unset points after each batch
        self.point = []
        logger.info("End of batch")

if __name__ == "__main__":
    agent = Agent()
    h = SimpleUDF(agent)
    agent.handler = h
    agent.start()
    agent.wait()
