package statistics

import (
	"database/sql"
	"fmt"
	"git.woa.com/woodyuan/woodSE/database"
	"strings"
)

func (l *TopNList) Update(syno *synopsis) {

	if syno.Father.Next == nil {
		OldFather := syno.Father
		NewFather := NewBucket(OldFather.Freq + 1)
		OldFather.Next = NewFather
		NewFather.Pre = OldFather

		syno.Father = NewFather
		NewFather.child = syno
		NewFather.childTail = syno

		if OldFather.child == syno {
			if syno.Next == nil {
				defer l.Delete(OldFather)
			} else {
				OldFather.child = syno.Next
				syno.Next.Pre = nil
			}
		} else {
			syno.Pre.Next = syno.Next
			if syno.Next != nil {
				syno.Next.Pre = syno.Pre
			}

		}
		syno.Pre = nil
		syno.Next = nil
	} else if syno.Father.Next.Freq != syno.Father.Freq+1 {
		temp := syno.Father.Next
		OldFather := syno.Father
		NewFather := NewBucket(OldFather.Freq + 1)

		OldFather.Next = NewFather
		NewFather.Pre = OldFather
		NewFather.Next = temp
		temp.Pre = NewFather

		NewFather.child = syno
		syno.Father = NewFather
		NewFather.childTail = syno

		if OldFather.child == syno {
			if syno.Next == nil {
				defer l.Delete(OldFather)
			} else {
				OldFather.child = syno.Next
				syno.Next.Pre = nil
			}
		} else {
			syno.Pre.Next = syno.Next
			if syno.Next != nil {
				syno.Next.Pre = syno.Pre
			}

		}
		syno.Pre = nil
		syno.Next = nil
	} else {
		OldFather := syno.Father
		if OldFather.child == syno {
			if syno.Next == nil {
				defer l.Delete(OldFather)
			} else {
				OldFather.child = syno.Next
				syno.Next.Pre = nil
			}
		} else {
			syno.Pre.Next = syno.Next
			if syno.Next != nil {
				syno.Next.Pre = syno.Pre
			}
		}
		syno.Father = OldFather.Next
		NewFather := syno.Father
		temp := NewFather.child
		NewFather.child = syno
		syno.Pre = nil
		syno.Next = temp
		temp.Pre = syno

	}
}
func (l *TopNList) Delete(Bucket *TopNBucket) {
	Bucket.Pre.Next = Bucket.Next
	Bucket.Next.Pre = Bucket.Pre

}
func (l *TopNList) Replace(ID interface{}) {
	temp := l.head.Next.childTail
	if l.head.Next.childTail.Pre != nil {
		l.head.Next.childTail = l.head.Next.childTail.Pre
	}
	delete(l.m, temp.ID)
	temp.ID = ID
	temp.Error = temp.Father.Freq
	l.m[ID] = temp
	l.Update(temp)

}
func (l *TopNList) Add(ID interface{}) {
	syno := new(synopsis)
	syno.ID = ID
	syno.Error = 0
	l.m[ID] = syno
	if l.head.Next == nil {
		newBucket := NewBucket(1)
		l.head.Next = newBucket
		newBucket.Pre = l.head

		newBucket.childTail = syno
	} else if l.head.Next.Freq != 1 {
		temp := l.head.Next
		newBucket := NewBucket(1)
		l.head.Next = newBucket
		newBucket.Pre = l.head

		newBucket.Next = temp
		temp.Pre = newBucket

		newBucket.childTail = syno
	}
	temp := l.head.Next.child
	syno.Father = l.head.Next
	l.head.Next.child = syno

	syno.Next = temp
	if temp != nil {
		temp.Pre = syno
	}
}

func (l *TopNList) Build(res *sql.Rows) {

	var value []byte
	i := 0

	for res.Next() && i < 1 { //init BS
		err := res.Scan(&value)
		if err != nil {
			//log.Println(err.Error())
			return
		}
		if len(value) == 0 {
			l.BS.NumOfNULL += 1
			l.BS.TotalRows += 1
			continue
		}

		Types := strings.Split(l.Type, " ")

		var TypeValue interface{}
		if Types[0] == "INTEGER" {
			if Types[1] == "UNSIGNED" {
				TypeValue = database.NullUint(value)
			} else {
				TypeValue = database.NullInt(value)
			}
			l.NDVCounter.Add(integerHash(database.NullUint(value)))
		} else if Types[0] == "FLOAT" {
			TypeValue = database.NullFloat(value)
			l.NDVCounter.Add(StringHash(database.NullString(value)))
		} else {
			TypeValue = database.NullString(value)
			l.NDVCounter.Add(StringHash(TypeValue.(string)))
		}

		l.BS.MAX = TypeValue
		l.BS.MIN = TypeValue

		if syno, ok := l.m[TypeValue]; ok {
			l.Update(syno)
		} else {
			l.Add(TypeValue)

		}
		i++
	}
	for res.Next() && i < l.size {

		err := res.Scan(&value)
		if err != nil {
			//log.Println(err.Error())
			return
		}
		if len(value) == 0 {

			l.BS.NumOfNULL += 1
			l.BS.TotalRows += 1
			continue
		}
		Types := strings.Split(l.Type, " ")

		var TypeValue interface{}
		if Types[0] == "INTEGER" {
			if Types[1] == "UNSIGNED" {
				TypeValue = database.NullUint(value)
			} else {
				TypeValue = database.NullInt(value)
			}
			l.NDVCounter.Add(integerHash(database.NullUint(value)))
		} else if Types[0] == "FLOAT" {
			TypeValue = database.NullFloat(value)
			l.NDVCounter.Add(StringHash(database.NullString(value)))
		} else {
			TypeValue = database.NullString(value)
			l.NDVCounter.Add(StringHash(TypeValue.(string)))
		}

		l.BS.Gather(TypeValue, l.Type)

		if syno, ok := l.m[TypeValue]; ok {
			l.Update(syno)
		} else {
			l.Add(TypeValue)
			i++
		}
	}
	for res.Next() {
		err := res.Scan(&value)
		if err != nil {
			//log.Println(err.Error())
			return
		}
		if len(value) == 0 {
			l.BS.NumOfNULL += 1
			l.BS.TotalRows += 1
			continue
		}

		Types := strings.Split(l.Type, " ")

		var TypeValue interface{}
		if Types[0] == "INTEGER" {
			if Types[1] == "UNSIGNED" {
				TypeValue = database.NullUint(value)
			} else {
				TypeValue = database.NullInt(value)
			}
			l.NDVCounter.Add(integerHash(database.NullUint(value)))
		} else if Types[0] == "FLOAT" {
			TypeValue = database.NullFloat(value)
			l.NDVCounter.Add(StringHash(database.NullString(value)))
		} else {
			TypeValue = database.NullString(value)
			l.NDVCounter.Add(StringHash(TypeValue.(string)))
		}

		l.BS.Gather(TypeValue, l.Type)

		if syno, ok := l.m[TypeValue]; ok {
			l.Update(syno)
		} else {
			l.Replace(TypeValue)
		}
	}

	l.BS.NDV = l.NDVCounter.Count()
	l.BS.AverageLength = uint64(float64(l.BS.AverageLength) / float64(l.BS.TotalRows))
}

func (l TopNList) Show() {
	count := 0
	temp := l.head.Next
	fmt.Println("column   counts")
	for temp.Next != nil {
		temp = temp.Next
	}
	for temp != l.head {
		ch := temp.child
		for ch != nil {
			fmt.Print(ch.ID)
			fmt.Print("    ")
			fmt.Print(temp.Freq - ch.Error)

			fmt.Println(" ")
			count++
			if count > 10 {
				fmt.Println("NDV为：", l.NDVCounter.Count())
				return
			}
			ch = ch.Next
		}
		temp = temp.Pre
	}

}
