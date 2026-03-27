# Tech Stock Project
## What is it?
The purpose of this project is to make it more efficient for log-term hedge fund analysts to analyse the market of the top tech stocks. This includes a dashboard with the following features: 
-  Easy, graphical comparison of metrics across different tech companies; 
-  A specific searching feature to find a particular, singular tech stock, in order to view a plain-english summary for it, as well as any other graphs related to that stock
-  A chatbot to allow users to further query displayed information, including citing it's resources, giving users further flexibility, to view entire sources they may be interested in, as well as for trust and transparency

---

ETL Pipelines for
* RSS Feeds
* Reddit
* Alpacha

---

## Testing features

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
