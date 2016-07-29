# -*- coding: utf-8 -*-
"""
PROGRAMACIÓN PARALELA 14/15

@Luis Miguel Barbero Juvera

Integracion de los datos
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
        for sesion, pagina in datos:
            yield (usuario, num_total, sesion), pagina
        
    def comportamiento(self, (usuario, num_total, sesion), pagina):
        comportamiento = set()
        for i in pagina:
            comportamiento.add(i)
        yield (usuario, num_total, list(comportamiento)), 1
        
    def freq(self, (usuario, num_total, comportamiento), frecuencia):
        yield comportamiento, (sum(frecuencia), usuario, num_total)
        
    def usuarios(self, comportamiento, datos):
        lista_usuarios = set()
        for freq, usuario, num_total in datos:
            lista_usuarios.add(usuario)
            yield comportamiento, (freq, usuario, num_total)
        yield comportamiento, ("._lista_de_usuarios_.", list(lista_usuarios), None)

    def reagrupar(self, comportamiento, datos):
        dato, usuario, num_total = datos.next()
        assert dato == "._lista_de_usuarios_."
        lista_usuarios = usuario
        for freq, usuario, num_total in datos:
            yield (usuario, num_total), (comportamiento, freq, lista_usuarios)
            
    def no_reps(self, (usuario, num_total), datos):
        for comportamiento, freq, lista_usuarios in datos:
            if usuario in lista_usuarios:
                lista_usuarios.remove(usuario)
            yield (usuario, num_total), (comportamiento, freq, lista_usuarios)
                       
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.total),
            MRStep(reducer = self.comportamiento),
            MRStep(reducer = self.freq),
            MRStep(reducer = self.usuarios),
            MRStep(reducer = self.reagrupar),
            MRStep(reducer = self.no_reps)
        ]
        
if __name__ == '__main__':
    MRprueba.run()
