<p align="center">
  <img width="200" src="./assets/logo.png" />
</p>

<div align=center>
<a href="https://test.pypi.org/project/mictlanx/"><img src="https://img.shields.io/badge/build-0.0.131-2ea44f?logo=Logo&logoColor=%23000" alt="build - 0.0.131"></a>
</div>
<div align=center>
	<h1>ActiveX: <span style="font-weight:normal;"> High available active objects</span></h1>
</div>

<!-- #  MictlanX  -->
**ActiveX** is a prototype active object system for intensive application. For now the source code is kept private, and it is for the exclusive use of the *Muyal-ilal* research group. 

## Prerequisites 🧾

- Install [Poetry](https://python-poetry.org/)
- Install Pip dependencies
  ```bash
  pip3 install -r requirements.txt
  ```
- Install Docker for your OS

<p align="right">(<a href="#top">back to top</a>)</p>


## Getting started 🚀

Following the next steps to run the example a simple calculator.

```python
from activex.activex import ActiveX
from activex.activex.decorators import activex
from typing import List

class Calculator(ActiveX):
    def __init__(self):
      self.records:List[str] =[]

    @activex
    def add(self,x:int,y:int):
      res = x+y
      self.records.append("Add {} + {} = {}".format(x,y,res))
      return res

# It is very important to call the activex handler
_ = ActiveXHandler.local()
calc = Calculator()
# Do some operations
res = calc.add(10,10) # -> 20
# Save the object
calc.persistify()
```


## Steps 
### Step 1. Deploy MictlanX
```
./run_mictlanx.sh
```
Once all services are up  and running execute the next bash script
```
./ini_peers.sh && ./peers_status.sh
```

### Step 2. Init the virtualenviroment and install

```bash
poetry shell && poetry install
```
### Step 3. Run examples

Put a new object in MictlanX
```bash
python3 examples/01_put_calculator.py mycalculator01
```
Get a object from MictlanX and use it in ActiveX
```bash
python3 examples/02_get_calculator.py mycalculator01
```

<p align="right">(<a href="#top">back to top</a>)</p>


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
