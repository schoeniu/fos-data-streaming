import pandas as pd

# might need to adjust the path backslashes when running on unix systems
file_location = '.\\data\\dev-2019-Oct.csv'

def main():
    df = pd.read_csv(file_location)

    df['datetime'] = pd.to_datetime(df['event_time'])
    df = df.set_index(pd.DatetimeIndex(df['datetime']))

    df = df.drop(['event_time','user_session'], axis=1)

    df_view = df[df['event_type']=="view"]
    df_cart = df[df['event_type']=="cart"]
    df_purchase = df[df['event_type']=="purchase"]

    print(df.resample('H', on='datetime').agg({
        'event_type':'count'
    }).rename(columns={'event_type': 'Number of events'}))

    print(df_view.resample('H', on='datetime').agg({
        'event_type':'count'
    }).rename(columns={'event_type': 'Number of items viewed'}))

    print(df_cart.resample('H', on='datetime').agg({
        'event_type':'count'
    }).rename(columns={'event_type': 'Number of items put in cart'}))

    print(df_purchase.resample('H', on='datetime').agg({
        'event_type':'count',
        'price':'sum'
    }).rename(columns={'event_type': 'Number of items sold','price': 'Value of items sold'}))

if __name__ == '__main__':
    main()