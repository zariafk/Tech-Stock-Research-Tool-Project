<<<<<<< HEAD
ETL Pipelines for
* RSS Feeds
* Reddit
* Alpacha



# Testing 

**Pytest fixture**: 
A reusable piece of test data or setup that multiple tests can use. 
Instead of copying the same fake data into every test, you define it once as a fixture and every test can request it. 
Keeps tests DRY (don't repeat yourself).

**Namespace**: 
In the test file, when defining a fixture with @pytest.fixture, its created it at the module level. 
Any test function can then request it by adding it as a parameter. 
The namespace is where that fixture lives — in this case, the test module.

**MagicMock**:
A fake object that pretends to be something else. 
When you patch a function with MagicMock, you're saying whenever this real function is called during the test, use my fake version instead.
=======
# Tech Stock Dashboard
>>>>>>> a12f5fbc44a7b0c0fd5058fb55db286fb363ae7e
