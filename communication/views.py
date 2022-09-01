from django.shortcuts import render

def index(request):
    return render(request, 'communication/index.html')

def room(request, room_name):
    return render(
        request, 'communication/room.html', {
            'room_name': room_name
        }
    )