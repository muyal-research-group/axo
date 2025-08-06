<p align="center">
  <img width="200" src="./assets/logo.png" />
</p>

<div align=center>
<a href="https://test.pypi.org/project/mictlanx/"><img src="https://img.shields.io/badge/build-0.0.20-2ea44f?logo=Logo&logoColor=%23000" alt="build - 0.0.20"></a>
</div>
<div align=center>
	<h1>Axo: <span style="font-weight:normal;"> High Available Execution Engine</span></h1>
</div>

**Axo** is a high available execution engine of the Axo platform, responsible for managing, executing, and orchestrating Active Objects (AO). The AO that encapsulates both data and behavior and can be executed remotely or locally like serverless functions.
<!-- **Axo** is a prototype active object system for intensive application. For now the source code is kept private, and it is for the exclusive use of the *Muyal-ilal* research group.  -->


<p align="center">
  <!-- <img width="750" src="./assets/activex_01.png" /> -->
  <img width="750" src="./assets/arch.gif" />
</p>


## Prerequisites 🧾
Before using or developing with Axo, ensure the following tools are installed and configured:

### 1. System Requirements
  - Python ≥ 3.9
  - Docker ≥ 28.3.2
### 2. Install Python Dependencies (Quick Start)
- Install [Poetry](https://python-poetry.org/)
  ```bash
  pip3 install -r requirements.txt
  ```
### 3. Development Environment (Recommended)
For development and contribution, we recommend using Poetry for environment and dependency management.

- Install poetry shell
  ```
  poetry self add poetry-plugin-shell
  ```
- Init a new virtual environment 
  ```
  poetry shell
  ```
- Install the dependencies
  ```
  poetry lock & poetry install
  ```

<p align="right">(<a href="#top">back to top</a>)</p>

### 4. Distributed mode 
For distributed mode you must deploy a storage service and at least one ```axo-endpoint```

```bash
chmod +x ./run_storage.sh && chmod +x ./run_endpoint.sh  && ./deploy_storage.sh && deploy_endpoint.sh
```

## Getting started 🚀

Following the next steps to run the example a simple calculator.

```python
from axo import Axo,axo_method
from typing import List
from axo.contextmanager import ActiveXContextManager
from axo.endpoint.manager import DistributedEndpointManager

dem = DistributedEndpointManager()
dem.add_endpoint(
    endpoint_id="activex-endpoint-0",
    hostname="localhost",
    protocol="tcp",
    req_res_port=16667,
    pubsub_port=16666
)

acm = ActiveXContextManager.distributed(
    endpoint_manager=dem
)

class Calculator(Axo):
    def __init__(self):
      self.records:List[str] =[]

    @axo_method
    def add(self,x:int,y:int):
      res = x+y
      self.records.append("Add {} + {} = {}".format(x,y,res))
      return res

# It is very important to call the activex handler


calc = Calculator()
# Do some operations
res = calc.add(10,10) # -> 20
# Save the object
calc.persistify()
```



## Examples

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

 Alejandro Zequeira - [@AlejandroZequeria]() - alejandro.delarosa@cinvestav.mx (Main developer)

 Ignacio Castillo - [@NachoCastillo]() - jesus.castillo.b@cinvestav.mx (Software Architect / Design)

<p align="right">(<a href="#top">back to top</a>)</p>
