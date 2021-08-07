# Analyse your own spotify music habits
The purpose of this code is to support anyone who fancies analysing their own Spotify music, because why not. It can be tricky getting all of the data you might want, so I have done the hard work for you!

It uses the Last.fm API to call basic information on the latest music you have listened too.

A number of further APIs are then used to enhance the dataset: 
- MBID: gets the music ID of a particular track so it can be linked to other data sources
- MusicBrainz: a music encyclopedia that returns music metadata incl. genres and locations
- SpotiPY: provides further information about tracks, incl. tags and features (e.g. popularity stats, acousticness, danceability)
- Nominatim (OpenStreetMap) + Google Maps: geocode tracks and get lat/lon coordinates of locations


I have only included code that will allow you to pull the data - the analysis is up to you! But here are some suggestions:
- Validate your 'Spotify Unwrapped' stats (although maybe be a bit more ambitious)
- Figure out your go to music genres for your different moods
- Create the ultimate music quiz with your friend's spotify data (with their consent), and figure out who is the truest Lizzo fan once and for all
- Push playlists with coded messages to your friend's Spotify (but again, get their consent)
- Identify your most effective music genre/playlist for running (e.g. add in data from your fitbit/smart watch)



# Pre-requisities

### Required Skills:
Most of the more complex code is hidden in functions within the .py scripts. A basic python understanding should be enough to run the .ipynb notebooks to pull the data and conduct your own analysis. 

### Required Packages
- PyLast
- Geopy
- Spotipy 

### Other Requirements:
- Spotify Account
- LastFM account
    - You pull your spotify data through LastFM. It only starts gathering your Spotify data once you have signed up.

# Run through terminal 
1. Clone the repo locally
2. Within the cloned folder execute `python PlayGame`
3. Enjoy the game!     


 

