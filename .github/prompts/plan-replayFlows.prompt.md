Plan: Replay Flows Page

1) Data access and options
- Add a new options class for replay flows stored procedures and timeout.
- Add repository methods:
  - GetFailedFlowsAsync(pnlDate) to call administration.GetFailedFlows with @PnlDate.
  - ReplayFlowsAsync(rows, userId) to call Replay.UspInsertReplayFlows with @UserId and a table-valued parameter of type Replay.ReplayAdjAtCoreSet.
- Register the repository and options in Program.cs.

2) Models and table mapping
- Define a model for failed flow rows with all columns from administration.GetFailedFlows.
- Map columns to a grid row view model that includes a selection flag and editable fields for SkipCoreProcess and Droptabletpm.
- Format dates to dd-mm-yyyy and numeric values using existing helpers.

3) UI page and layout
- Create Components/Pages/ReplayFlows.razor and .razor.cs with route /replay-flows.
- Add a PnlDate textbox at the top (YYYY-MM-DD) and a Refresh button.
- Render a grid with a left selection checkbox per row and all columns.
- Use checkbox editors for SkipCoreProcess and Droptabletpm; enable edits only for selected rows.
- Add Submit button to send selected rows.
- Show success/error message and refresh grid on success.

4) Navigation
- Add a NavMenu link to Replay Flows.

5) Validation and UX
- Require a valid PnlDate before loading/submitting; show inline validation.
- Disable Submit when no rows selected or while submitting.
- Handle empty results and error states similarly to Monitoring.

6) Styling
- Reuse existing table styles; add small scoped styles for checkbox alignment if needed.

7) Verification
- Build the app; open Replay Flows page; verify load, selection, edits, and submit behavior.
