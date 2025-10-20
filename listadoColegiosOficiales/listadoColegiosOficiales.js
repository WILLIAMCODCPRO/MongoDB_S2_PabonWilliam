// use listadoColegiosOficiales

db.createCollection("coleccionTemporal"); // Creamos una coleccion temporal

db.coleccionTemporal.updateMany({},{$rename:{"a��o":"anio"}}); // Corregimos el nombre año

// Crear las colecciones
db.createCollection("establecimientosEducativos");
db.createCollection("telefonos");
db.createCollection("direcciones");
db.createCollection("correosElectronicos");
db.createCollection("tiposEstablecimiento");
db.createCollection("niveles");
db.createCollection("jornadas");
db.createCollection("especializaciones");
db.createCollection("aniosInformacionReportada");
db.createCollection("zona");
db.createCollection("rector");
db.createCollection("grado");
db.createCollection("nivelEstablecimiento");
db.createCollection("jornadaEstablecimiento");
db.createCollection("especializacionEstablecimiento");
db.createCollection("gradoEstablecimiento");

// Agregar datos a la coleccion niveles

db.coleccionTemporal.aggregate([
  {
    $project: {
      niveles: { $split: ["$niveles", ","] }  
    }
  },
  { $unwind: "$niveles" },
  {
    $group: {
      _id: "$niveles" 
    }
  },
  {
    $project: {
      _id: 0,
      nivel: "$_id" 
    }
  },
  {
    $out: "niveles"
  }
]);

// Agregar datos a la coleccion zona

db.coleccionTemporal.aggregate([

    {
    $project: {
      zona: { $split: ["$zona", ","] }  
    }
  },

  { $unwind: "$zona" },

    {
        $group:{
            _id: "$zona"
        }
    },

    {
    $project: {
      _id: 0,
      zona: "$_id" 
    }
  },
  {
    $out: "zona"
  }

]);


// Agregar datos a la coleccion grado

db.coleccionTemporal.aggregate([

    {
    $addFields: {
      grados: { $toString: "$grados" }
    }
  },

    {
    $project: {
      grados: { $split: ["$grados", ","] }  
    }
  },

  { $unwind: "$grados" },

  

  {
    $match:{
        grados: { $in: ["1", "2","3","4","5","6","7","8","9","10","11"] }

    }

  },

    {
        $group:{
            _id: "$grados"
        }
    },

    {
    $project: {
      _id: 0,
      grado: "$_id" 
    }
  },
  {
    $out: "grado"
  }

]);

// Agregar datos a la coleccion rector

db.coleccionTemporal.aggregate([
  {
    $group: {
      _id: "$nombre_Rector" 
    }
  },
  {
    $project: {
      _id: 0,
      nombreRector: "$_id" 
    }
  },
  {
    $out: "rector"
  }
]);

// Agregar datos a la coleccion direcciones

db.coleccionTemporal.aggregate([
  {
    $group: {
      _id: "$direccion" 
    }
  },
  {
    $project: {
      _id: 0,
      direccion: "$_id" 
    }
  },
  {
    $out: "direcciones"
  }
]);

// Agregar datos a la coleccion telefonos

db.coleccionTemporal.aggregate([
  
    {
    $addFields: {
      telefono: { $toString: "$telefono" }
   }
  },


  {
    $project: {
      telefono: {
        $split: [
          {
              input: {
                $replaceAll: {
                  input: {
                    $replaceAll: {
                      input: "$telefono",
                      find: "--",
                      replacement: ","
                    }
                  },
                  find: "/",
                  replacement: ","
                }
              },
            },
          ","
        ]
      }
    }
  },

  

  {
    $group: {
      _id: "$telefono" 
    }
  },
  {
    $project: {
      _id: 0,
      telefono: "$_id" 
    }
  },
  {
    $out: "telefonos"
  }
]);

// Agregar datos a la coleccion correosElectronicos

db.coleccionTemporal.aggregate([
  {
    $project: {
      correo_Electronico: { $split: ["$correo_Electronico", "--"] }  
    }
  },
  { $unwind: "$correo_Electronico" },
  {
    $group: {
      _id: "$correo_Electronico" 
    }
  },
  {
    $project: {
      _id: 0,
      correoElectronico: "$_id" 
    }
  },
  {
    $out: "correosElectronicos"
  }
]);

// Agregar datos a la coleccion tiposEstablecimiento

db.coleccionTemporal.aggregate([

  {
  $project: {
    tipo_Establecimiento: { $split: ["$tipo_Establecimiento", ","] }  
  }
},

{ $unwind: "$tipo_Establecimiento" },

  {
    $group:{
      _id: "$tipo_Establecimiento"
    }
  },

  {
  $project: {
    _id: 0,
    tipoEstablecimiento: "$_id" 
  }
},
{
  $out: "tiposEstablecimiento"
}
]);

// Agregar datos a la coleccion aniosInformacionReportada

db.coleccionTemporal.aggregate([

  {
    $group:{
      _id: {anio: {$year: "$anio"}}
    }
  },
                                           
  {
  $project: {
    _id: 0,
    anio: "$_id" 
  }
},
{
  $out: "aniosInformacionReportada"
}
]);

// Agregar datos a la coleccion especializaciones

db.coleccionTemporal.aggregate([

  {
  $project: {
    especialidad: { $split: ["$especialidad", ","] }  
  }
},

{ $unwind: "$especialidad" },

  {
    $group:{
      _id: "$especialidad"
    }
  },

  {
  $project: {
    _id: 0,
    especializacion: "$_id" 
  }
},
{
  $out: "especializaciones"
}
]);

// Agregar datos a la coleccion jornadas

db.coleccionTemporal.aggregate([

  {
  $project: {
    jornadas: { $split: ["$jornadas", ","] }  
  }
},

{ $unwind: "$jornadas" },

  {
    $group:{
      _id: "$jornadas"
    }
  },

  {
  $project: {
    _id: 0,
    jornada: "$_id" 
  }
},
{
  $out: "jornadas"
}
]);

// Agregar datos a la coleccion establecimientosEducativos

db.coleccionTemporal.aggregate([
  {
    $group:{
      _id:{
        nombreEstablecimiento: "$nombreestablecimiento",
        numeroSedes: "$numero_de_Sedes"
      }
    }
  },

  {
    $project:{
      _id:0,
      nombreEstablecimiento:"$_id.nombreEstablecimiento",
      numeroSedes: "$_id.numeroSedes"
    }
  },

  {
    $out: "establecimientosEducativos"
  }
]);