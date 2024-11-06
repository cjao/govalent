# Govalent

Govalent aims to implement the Covalent dispatcher in Go. The main archiectural
difference from the original Python version is that executor plugins are
decoupled into standalone processes that interact with the main dispatcher
through REST APIs.

# Highlights

- Dispatcher driven entirely by REST API
- Standalone executors that implement a common REST API
