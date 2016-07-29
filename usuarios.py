# -*- coding: utf-8 -*-
"""
PROGRAMACIÓN PARALELA 14/15

@Luis Miguel Barbero Juvera

Devolver para cada comportamiento detectado una lista de usuarios que lo han seguido
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
                yield (usuario, sesion), pagina
            else:
                sesion = sesion + 1
                cota = momento+tiempo_sesion
                yield (usuario, sesion), pagina

    def comportamiento(self, (usuario, sesion), pagina):
        comportamiento = set()
        for i in pagina:
            comportamiento.add(i)
        yield list(comportamiento), usuario
        
    def usuarios(self, comportamiento, usuario):
        lista_usuarios = set()
        for user in usuario:
            lista_usuarios.add(user)
        yield comportamiento, list(lista_usuarios)
        
                       
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.comportamiento),
            MRStep(reducer = self.usuarios)
        ]
        
if __name__ == '__main__':
    MRprueba.run()
