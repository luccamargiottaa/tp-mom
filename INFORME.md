## Estructura de archivos

- **factory.go**: Define los constructores públicos de los middlewares.
- **queue.go**: Define la implementación de la interfaz middleware para una *Queue*.
- **exchange.go**: Define la implementación de la interfaz middleware para un *Exchange*.
- **helpers.go**: Define funciones comunes del uso del middleware *RabbitMQ*.

## Decisiones de diseño

- Las implementaciones del middleware no están hechas para ser compartidas entre threads.
- Se omitió el uso de *Publisher confirms* por su costo en latencia, manteniendo escalabilidad.
- Se optó por estructuras durables y mensajes persistentes para permitir la recuperación del *Broker* ante reinicios, con posibles pérdidas de mensajes en vuelo.
- Se utilizó la función de **requeue** ante un *Nack* para permitir el reenvío del mensaje a otro consumidor que pueda recibirlo.
- Se seteó un **prefetch_count** de **1** en la *Queue* para maximizar el fairness.
- Se eligió un *Exchange* de tipo *Direct* para tener un *routing* simple.
- No se evita el duplicado de un mensaje cuando un suscriptor está suscrito a más de una *key* con las cuales el mensaje fue enviado. Se considera el comportamiento esperado y debería evitarse mediante un manejo correcto de las *keys*.
- Se usaron colas exclusivas en el *Exchange*, aceptando la posible pérdida de mensajes siguiendo el modelo *Publisher-Subscriber*.
