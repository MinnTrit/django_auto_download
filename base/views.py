from django.shortcuts import render, redirect
from django.http import HttpResponse
from .tasks import run_scrapper
import logging
from redis import Redis

logger = logging.getLogger(__name__)

# Create your views here.
def home(request):
    if request.method == 'GET':
        return render(request, 'base/home.html')
    elif request.method == 'POST':
        start_date = request.POST.get('start_date')
        end_date = request.POST.get('end_date')
        seller_id = request.POST.get('seller_id')
        launcher = request.POST.get('launcher')
        seller_ids_list = [item.strip() for item in seller_id.split(',')]
        try:
            run_scrapper(start_date, end_date, seller_ids_list, launcher)
            return redirect('home')
        except Exception as e:
            print(f'Error occured as {e}')
            logger.error(f'Error occurred: {e}')
            return HttpResponse('Failed to launch jobs')
