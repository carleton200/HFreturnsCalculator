Liquidity and CRSPR Meeting notes



**Liquidity**

* Liquidity - there are locks on the ivnestments for when an how much you can withdraw
* Example with 25% withdraw per quarter:
*  	Decide at BOY for withdarw, each quarter is 25% of the original. 1 mil > .75 mil > 0.5 mil > 0.25 mil > 0



Building Myself:

* Liquidy events: timeline liquidity data
* eveyr fund has a fund class. The Liquidity term resides here.Fund class name from 'Funds' corresponds to 'Fund' In Liquidity events
* Use the inception field that I already make (first capital call). If no gate withdraw everything with 90 day notice. If gated, must wait until the gate lock period ends. If quarterly, must be inital day of quarter, Month is initial day of months.
* Liquidy terms in 'Views' has all the fields
* Be able to display by asset class and pool aggregates
* Make report look like 'Liquidity Schedule by Quarter' in Dynamo









**Performance**

Add PME, IRR, Distribtuions in dollars, contri\_\_\_\_something in dollars, TVPI, DPI



Current state:





Thought:

only show DPI, TVPI and IRR at the pool level from the investor if investor mode

If Full Portfolio, show everything at fund level.

If investor mode, hide DPI, TVPI, and IRR at all but the pool level





For full portfolio, funds are pool to fund. Aggregates can be combined from lower levels as the values are consistent

For investor level, just grab all by the pool level if grouped by pool





Other headers

For full portfolio, have commitment, unfunded (treat similar as TVPI and DPI. track contributions an distributions at the fund level. can aggregate)

For investor mode, have contributions to the pool level only (use the new field to see whether contributions or distributions from transactions are real. Check this can remove contributions from partner transfers out so the new investing entity has the contributions)





Paid in capital - contributions

