# AutoCops

Sometimes you just gota copy a bunch of junk all over the place.

## Dependencies
* Fabirc
* Watchdog

## Configuration

Setup your config file with a list of sources and destinations, like so:

```
{
    "tmbg": [
        {
            "source": "C:/Users/jlinnell/dev/autocops",
            "dest": [
                {
                    "host": "theymightbegiants.com",
                    "path": "/home/jflansburgh/temp/autocops"
                }
            ]
        }
    ]
}
```

## Running

To list out your different configuration sections, run

```
python3 autocops.py -p
```

To run a particular configuration section, use:

```
python3 autocops.py -p tmbg
```

