package user

import (
    "gorm.io/gorm"
)

type Service struct {
    DB *gorm.DB
}

func NewService(db *gorm.DB) *Service {
    return &Service{DB: db}
}

func (s *Service) Register(email, password string) error {
    user := User{Email: email, Password: password}
    return s.DB.Create(&user).Error
}

func (s *Service) FindByEmail(email string) (*User, error) {
    var u User
    err := s.DB.Where("email = ?", email).First(&u).Error
    return &u, err
}
