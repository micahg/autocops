# AutoCops

Sometimes you just gota copy a bunch of junk all over the place.

## Dependencies
* Fabirc
* Watchdog

## Configuration

Setup your config file with a list of sources and destinations, like so:

```
{
    "source": "C:/Users/jlinnell/dev/autocops",
    "dest": [
        {
            "host": "192.168.1.250",
            "path": "/home/jflansburgh/temp/autocops"
        }
    ]
}
```