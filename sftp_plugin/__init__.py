from fman import DirectoryPaneCommand, show_alert

class SayHi(DirectoryPaneCommand):
	def __call__(self):
		show_alert('Hello World!')