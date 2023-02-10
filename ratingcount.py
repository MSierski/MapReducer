from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    # def mapper(self, _, line):
    #     (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
    #     result = [rating, 1]
    #     yield result

    # def reducer(self, key, value):
    #     result = [key, sum(value)]
    #     yield result


     def mapper(self, _, line):
         splits = line.split(",")
         if len(splits) == '4':    
            result = [movieId,('R', float(rating))]
            yield result                  
         if len(splits) == '3':
            result = [movieId,('M', title)] 
            yield result 
          
     def reducer(self, key, value):
        x = 0
        y = 0
        for v in value:
         if v[0] == 'R':
            x += v[1]
            y += 1
         else:
            title = v[1]

        yield [title, x/y] 


if __name__ == '__main__':
    MRHotelRaitingCount.run()



 #https://gist.github.com/rjurney/2f350b2cbed9862b692b   