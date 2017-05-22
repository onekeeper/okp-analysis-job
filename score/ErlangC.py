#!/usr/bin/python
# -*- coding: UTF-8 -*-
###############################################################################
# ErlangC Script
# Created on 2017-01-09
# Author : Kevin Gideon
# Version: 1.0
# Usage:
###############################################################################


###############################################################################
# define functions
###############################################################################
from math import factorial
from decimal import Decimal

def ErlangC(servers, service_time):
    queues=1
    #servers=1
    system_arrival_rate=1.0
    response_time = 1
    myArray = []

    while response_time > 0:
        queue_arrival_rate = system_arrival_rate / queues
        queue_traffic = service_time * queue_arrival_rate

        rho = queue_traffic / servers
        ErlangB = queue_traffic / (1 + queue_traffic)

        m=0
        eb=0

        for m in range(2, servers, 1):
            eb = ErlangB
            ErlangB = eb * queue_traffic / ( m + eb * queue_traffic )

        ErlangC = ErlangB / (1 - rho + rho * ErlangB )
        utilization = ( service_time * queue_arrival_rate ) / servers

        if 1 - utilization == 0:
            break
        else:
            queue_time = ( ErlangC * service_time ) / ( servers * ( 1 - utilization ) )
        response_time =  service_time + queue_time
        queue_length = queue_arrival_rate * queue_time

        #print response_time, queue_time, system_arrival_rate
        list=[response_time,queue_time, system_arrival_rate]
        myArray.append(list)

        system_arrival_rate = float('%0.3f' %(system_arrival_rate + 0.1))

    return myArray


def GetInflectionPoint(servers, service_time):
    myArray = ErlangC(servers, service_time)
    maxIndex = len(myArray)

    idx = (maxIndex -4 if maxIndex -4 > 0 else 0)
    list = myArray[idx]
    #print list
    return list[2]

###############################################################################
# main test
###############################################################################
#GetInflectionPoint(8, 0.17)