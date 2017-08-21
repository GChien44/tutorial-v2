"""
This main python file contains the backend logic of the website, including
the reception of Cloud Pub/Sub messages and communication with GCS and datastore.
"""

# Import all necessary libraries.
import webapp2
import jinja2
import os
import logging
import json
import urllib
import collections
import cloudstorage as gcs
import googleapiclient.discovery
from google.appengine.ext import ndb
from google.appengine.ext import blobstore
from google.appengine.api import images

# Set up jinja2 for HTML templating.
template_dir = os.path.join(os.path.dirname(__file__), 'templates')
jinja_environment = jinja2.Environment(loader = jinja2.FileSystemLoader(template_dir))

# Constants. Note: Change THUMBNAIL_BUCKET and PHOTO_BUCKET to
# be applicable to your project.
THUMBNAIL_BUCKET = 'thumbnails-bucket'
PHOTO_BUCKET = 'shared-photo-album'
NUM_NOTIFICATIONS_TO_DISPLAY = 10
MAX_LABELS = 5

# Describes a Notification to be displayed on the home/news feed page.
# String message, date of posting, and generation number to prevent the
# display of repeated notifications.
class Notification(ndb.Model):
  message = ndb.StringProperty()
  date = ndb.DateTimeProperty(auto_now_add=True)
  generation = ndb.StringProperty()

# Describes a ThumbnailReference, which holds information about a given
# thumbnail (not the thumbnail itself).
# thumbnail_name: same as photo name.
# thumbnail_key: used to distinguish similarly named photos. Includes name and
# generation number.
# labels: list of label_names that apply to the photo.
# original_photo: url of the original photo, stored in GCS.
class ThumbnailReference(ndb.Model):
  thumbnail_name = ndb.StringProperty()
  thumbnail_key = ndb.StringProperty()
  date = ndb.DateTimeProperty(auto_now_add=True)
  labels = ndb.StringProperty(repeated=True)
  original_photo = ndb.StringProperty()

# Describes a Label, as assigned by Cloud Vision to photos.
# label_name: description of the label.
# labeled_thumbnails: thumbnail_keys of photos labeled with the label_name.
class Label(ndb.Model):
  label_name = ndb.StringProperty()
  labeled_thumbnails = ndb.StringProperty(repeated=True)

# Home/news feed page (notification listing).
class MainHandler(webapp2.RequestHandler):
  def get(self):
    # Fetch all notifications in reverse date order.
    notifications = Notification.query().order(-Notification.date).fetch(NUM_NOTIFICATIONS_TO_DISPLAY)
    template_values = {'notifications':notifications}
    template = jinja_environment.get_template("notifications.html")
    # Write to the appropriate html file.
    self.response.write(template.render(template_values))

# All photos page (displays thumbnails).
class PhotosHandler(webapp2.RequestHandler):
  def get(self):
    # Get thumbnail references from datastore in reverse date order.
    thumbnail_references = ThumbnailReference.query().order(-ThumbnailReference.date).fetch()
    # Build dictionary of img_url of thumbnail to thumbnail_references.
    thumbnails = collections.OrderedDict()
    for thumbnail_reference in thumbnail_references:
      img_url = get_thumbnail(thumbnail_reference.thumbnail_key)
      thumbnails[img_url] = thumbnail_reference
    template_values = {'thumbnails':thumbnails}
    template = jinja_environment.get_template("photos.html")
    # Write to appropriate html file.
    self.response.write(template.render(template_values))

# Search page.
class SearchHandler(webapp2.RequestHandler):
  def get(self):
    # Get search_term entered by user.
    search_term = self.request.get('search-term')
    # Obtain label applicable to search term.
    label = Label.query(Label.label_name==search_term).get()
    # Build dictionary of img_url of thumbnails to thumbnail_references that
    # have the given label.
    thumbnails = collections.OrderedDict()
    if label is not None:
      thumbnail_keys = label.labeled_thumbnails
      for thumbnail_key in thumbnail_keys:
        img_url = get_thumbnail(thumbnail_key)
        thumbnails[img_url] = ThumbnailReference.query(ThumbnailReference.thumbnail_key==thumbnail_key).get()
    thumbnails = collections.OrderedDict(reversed(list(thumbnails.items())))
    template_values = {'thumbnails':thumbnails}
    template = jinja_environment.get_template("search.html")
    # Write to appropriate html file.
    self.response.write(template.render(template_values))

# For receiving Cloud Pub/Sub push messages.
class ReceiveMessage(webapp2.RequestHandler):
  def post(self):
    logging.debug('Post body: {}'.format(self.request.body))
    message = json.loads(urllib.unquote(self.request.body).rstrip('='))
    attributes = message['message']['attributes']

    # Acknowledge message.
    self.response.status = 204

    # Gather and save necessary values from the Pub/Sub message.
    event_type = attributes.get('eventType')
    photo_name = attributes.get('objectId')
    generation_number = str(attributes.get('objectGeneration'))
    overwrote_generation = attributes.get('overwroteGeneration')
    overwritten_by_generation = attributes.get('overwrittenByGeneration')

    # Create the thumbnail_key using the photo_name and generation_number.
    # Note: Only photos with extension .jpg can be uploaded effectively.
    index = photo_name.index(".jpg")
    thumbnail_key = photo_name[:index] + generation_number + photo_name[index:]

    # Create the notification using the received information.
    new_notification = create_notification(photo_name, event_type, generation_number, overwrote_generation=overwrote_generation, overwritten_by_generation=overwritten_by_generation)

    # If the new_notification already has been stored, it is a repeat and can be
    # ignored.
    exists_notification = Notification.query(Notification.message==new_notification.message, Notification.generation==new_notification.generation).get()
    if exists_notification:
      return

    # Don't act for metadata update events.
    if new_notification.message == '':
      return

    # Store new_notification in datastore.
    new_notification.put()

    # For create events: shrink the photo to thumbnail size, store the thumbnail
    # in GCS, create a thumbnail reference and store it in datastore, and add
    # the thumbnail_key to the appropriate Labels.
    if event_type == 'OBJECT_FINALIZE':
      thumbnail = create_thumbnail(self, photo_name)
      store_thumbnail_in_gcs(self, thumbnail_key, thumbnail)
      original_photo = get_original(photo_name, generation_number)
      uri = 'gs://' + PHOTO_BUCKET + '/' + photo_name
      labels = get_labels(uri, photo_name)
      labels.append(photo_name)
      labels.append(photo_name[:index])
      thumbnail_reference = ThumbnailReference(thumbnail_name=photo_name, thumbnail_key=thumbnail_key, labels=labels, original_photo=original_photo)
      thumbnail_reference.put()

      add_thumbnail_reference_to_labels(labels, thumbnail_key)

    # For delete/archive events: remove the thumbnail_key from all applicable
    # Labels, delete the thumbnail from GCS, and delete the thumbnail_reference.
    elif event_type == 'OBJECT_DELETE' or event_type == 'OBJECT_ARCHIVE':
      remove_thumbnail_from_labels(thumbnail_key)
      delete_thumbnail(thumbnail_key)

    # No action performed if event_type is OBJECT_UPDATE

# Create notification.
def create_notification(photo_name, event_type, generation, overwrote_generation=None, overwritten_by_generation=None):
  if event_type == 'OBJECT_FINALIZE':
    if overwrote_generation is not None:
      message = photo_name + ' was uploaded and overwrote an older version of itself.'
    else:
      message = photo_name + ' was uploaded.'
  elif event_type == 'OBJECT_ARCHIVE':
    if overwritten_by_generation is not None:
      message = photo_name + ' was overwritten by a newer version.'
    else:
      message = photo_name + ' was archived.'
  elif event_type == 'OBJECT_DELETE':
    if overwritten_by_generation is not None:
      message = photo_name + ' was overwritten by a newer version.'
    else:
      message = photo_name + ' was deleted.'
  else:
    message = ''

  return Notification(message=message, generation=generation)

# Returns serving url for a given thumbnail, specified by the photo_name
# parameter.
def get_thumbnail(photo_name):
  filename = '/gs/' + THUMBNAIL_BUCKET + '/' + photo_name
  blob_key = blobstore.create_gs_key(filename)
  return images.get_serving_url(blob_key)

# Returns the url of the original photo.
def get_original(photo_name, generation):
  return 'https://storage.googleapis.com/' + PHOTO_BUCKET + '/' + photo_name + '?generation=' + generation

# Shrinks specified photo to thumbnail size and returns resulting thumbnail.
def create_thumbnail(self, photo_name):
  filename = '/gs/' + PHOTO_BUCKET + '/' + photo_name
  image = images.Image(filename=filename)
  image.resize(width=180, height=200)
  return image.execute_transforms(output_encoding=images.JPEG)

# Stores thumbnail in GCS bucket under name thumbnail_key.
def store_thumbnail_in_gcs(self, thumbnail_key, thumbnail):
  write_retry_params = gcs.RetryParams(backoff_factor=1.1)
  filename = '/' + THUMBNAIL_BUCKET + '/' + thumbnail_key
  with gcs.open(filename, 'w') as filehandle:
    filehandle.write(thumbnail)

# Deletes thumbnail from GCS bucket and deletes thumbnail_reference from
# datastore.
def delete_thumbnail(thumbnail_key):
  filename = '/gs/' + THUMBNAIL_BUCKET + '/' + thumbnail_key
  blob_key = blobstore.create_gs_key(filename)
  images.delete_serving_url(blob_key)
  thumbnail_reference = ThumbnailReference.query(ThumbnailReference.thumbnail_key==thumbnail_key).get()
  thumbnail_reference.key.delete()
  filename = '/' + THUMBNAIL_BUCKET + '/' + thumbnail_key
  gcs.delete(filename)

# Use Cloud Vision API to get labels for a photo.
def get_labels(uri, photo_name):
  service = googleapiclient.discovery.build('vision', 'v1')
  labels = []

  # Label photo with its name, sans extension.
  index = photo_name.index(".jpg")
  photo_name_label = photo_name[:index]
  labels.append(photo_name_label)


  service_request = service.images().annotate(body={
      'requests': [{
          'image': {
              'source': {
                'imageUri': uri
              }
          },
          'features': [{
              'type': 'LABEL_DETECTION',
              'maxResults': MAX_LABELS
          }]
      }]
  })
  response = service_request.execute()
  labels_full = response['responses'][0].get('labelAnnotations')

  ignore = ['of', 'like', 'the', 'and', 'a', 'an', 'with']

  # Add labels to the labels list if they are not already in the list and are
  # not in the ignore list.
  if labels_full is not None:
    for label in labels_full:
      if label['description'] not in labels:
        labels.append(label['description'])
        # Split the label into individual words, also to be added to labels list
        # if not already.
        descriptors = label['description'].split()
        for descript in descriptors:
          if descript not in labels and descript not in ignore:
            labels.append(descript)

  return labels

# Add given thumbnail_key to all applicable Labels or create new Labels if
# necessary.
def add_thumbnail_reference_to_labels(labels, thumbnail_key):
  for label in labels:
    label_to_append_to = Label.query(Label.label_name==label).get()
    if label_to_append_to is None:
      thumbnail_list_for_new_label = []
      thumbnail_list_for_new_label.append(thumbnail_key)
      new_label = Label(label_name=label, labeled_thumbnails=thumbnail_list_for_new_label)
      new_label.put()
    else:
      label_to_append_to.labeled_thumbnails.append(thumbnail_key)
      label_to_append_to.put()

# Remove the given thumbnail_key from all applicable Labels.
def remove_thumbnail_from_labels(thumbnail_key):
  thumbnail_reference = ThumbnailReference.query(ThumbnailReference.thumbnail_key==thumbnail_key).get()
  labels_to_delete_from = thumbnail_reference.labels
  for label_name in labels_to_delete_from:
    label = Label.query(Label.label_name==label_name).get()
    labeled_thumbnails = label.labeled_thumbnails
    labeled_thumbnails.remove(thumbnail_key)
    # If there are no more thumbnails with a given label, delete the label.
    if not labeled_thumbnails:
      label.key.delete()
    else:
      label.put()

app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/photos', PhotosHandler),
    ('/search', SearchHandler),
    ('/_ah/push-handlers/receive_message', ReceiveMessage)
], debug=True)
