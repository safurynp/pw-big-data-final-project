import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd

# Sample data (replace this with your dataset)
data = {
    'Category': ['A', 'B', 'C', 'D'],
    'Values': [10, 20, 15, 25]
}
df = pd.DataFrame(data)

# Initialize Dash app
app = dash.Dash(__name__)

# Create layout
app.layout = html.Div(children=[
    html.H1('Simple Dashboard'),
    dcc.Graph(
        id='example-graph',
        figure=px.bar(df, x='Category', y='Values', title='Bar Chart')
    )
])

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
