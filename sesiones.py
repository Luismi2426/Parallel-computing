# -*- coding: utf-8 -*-
"""
PROGRAMACIÓN PARALELA 14/15

@Luis Miguel Barbero Juvera

Devolver el numero de sesiones totales de cada usuario
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

#Tiempo de duracion de la sesion en segundos (>0)
tiempo_sesion = 3600

class MRprueba(MRJob):

    def mapper(self, _, linea):
        linea = linea.split()
        if not linea == []:
            usuario = linea[0]
            momento = time.mktime(time.strptime(linea[3][1:], "%d/%b/%Y:%H:%M:%S"))
            pagina = linea[6]
            codigo = linea[8]
            if codigo == '200':           
                yield usuario, (momento, pagina)
          
    def reducer(self, usuario, datos):
        sesion = 0
        cota = 0
        for momento, pagina in datos:
            if momento < cota:
                yield usuario, (sesion, pagina)
            else:
                sesion = sesion + 1
                cota = momento+tiempo_sesion
                yield usuario, (sesion, pagina)
        yield usuario, ("._num_total_.", sesion)
            
    def total(self, usuario, datos):
        sesion, dato = datos.next()
        assert sesion == "._num_total_."
        num_total = dato
        yield usuario, num_total
                       
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.total)
        ]
        
if __name__ == '__main__':
    MRprueba.run()
